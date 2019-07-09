local N = ...

local log   = require 'log'
local json  = require 'json'
local fiber = require 'fiber'
local stash = require 'stash'(...)
local val   = require 'val'

local GENERATION = package.reload.count

local permanent = stash.permanent or {}
local temporary = stash.temporary or {}
stash.permanent = permanent
stash.temporary = temporary

local fiber_val = val.idator({
	id     = '+function';
	name   = '+function';
	status = '+function';
})

local function is_fiber(fb)
	return pcall(fiber_val, fb)
end

local M = {
	_ = {
		permanent   = permanent;
		temporary   = temporary;

		--- need to be set implicitly
		--monitor_cfg = monitor_cfg;
		--on_event    = function () end;
	};
	_int = {};
}

local function default_cfg()
	return {
		period                 = 10;
		max_fibers             = 30;
		delay_seconds          = 600;
		csw_stuck_seconds      = 1200;
		heartrate_seconds      = -1;
		watchdog_lag           = 0.12;
		watchdog_period        = 0.1;
		bastards_allowed       = true;
		bastards_beats_allowed = false;
		bastards_masks         = {
			"^applier/";
			"^main$";
			"^console/unix";
			"^memtx%.";
			"^vinyl%.";
			"feedback_daemon";
			"checkpoint_daemon";
		};
	}
end

local cfg_validator = val.idator({
	period                 = val.opt(val.num);
	max_fibers             = val.opt(val.num);
	delay_seconds          = val.opt(val.num);
	csw_stuck_seconds      = val.opt(val.num);
	heartrate_seconds      = val.opt(val.num);
	bastards_allowed       = '?boolean';
	bastards_beats_allowed = '?boolean';

	-- list of strings, too validate correctly
	bastards_masks    = val.opt('table');
})

function M.configure(cfg)
	cfg = cfg or {}
	local ok, err = pcall(cfg_validator, cfg)
	for k, v in pairs(default_cfg()) do
		if cfg[k] == nil then
			cfg[k] = v
		end
	end
	M._.monitor_cfg = cfg
	return true
end

M.configure({})

local function _event(event_type, ...)
	if M._.on_event then
		return M._.on_event(event_type, ...)
	end
	return false
end

--- opts can have following keys
--  ttl_seconds - how much a fiber should live
--    if ttl_seconds value is less then 0, fiber is considered to be permanent and run during current package.reload.count
--    in other case fiber should end during the os.time() + ttl_seconds
--    be default ttl_seconds is equal to -1
--  delay_seconds - how much time should pass for permanent fiber to die after package.reload()
--    by default it's equal to monitor_cfg.delay_seconds
--  csw_stuck_seconds - how much time should pass with unchanged csw to consider fiber stuck
--    by default it's equal to monitor_cfg.csw_stuck_seconds
--  heartrate_seconds - how much time should pass without heartbeat from fiber
--    if heartrate_seconds is less then zero, fiber is considered to be monitored without heartbeats
--    by default it's equal to monitor_cfg.heartrate_seconds
--  fiber
--    fiber object to put on monitoring
--    by default it is equal to fiber.self()
--
--    fibers monitored with heartbeats are considered to send M.beat(fiber.self()) periodically
--    fibers monitored with heartbeats are also considered to end their life with M.done(fiber.self())

local opt_val = val.idator({
	ttl_seconds       = '?number';
	delay_seconds     = '?number';
	csw_stuck_seconds = '?number';
	heartrate_seconds = '?number';
})

function M.monitor(opts)
	opts = opts or {}
	local fb = opts.fiber or fiber.self()
	local ok, err = pcall(opt_val, opts)
	if not ok then
		local msg = ("Incorrect options passed to monitoring: %s"):format(err)
		_event("alert", "FBMONITOR_INCORRECT_USAGE", msg)
		error(msg, 2)
	end
	local ok, err = is_fiber(fb)
	if not ok then
		local msg = ("Fiber does not look like fiber: %s"):format(err)
		_event("alert", "FBMONITOR_INCORRECT_USAGE", msg)
		error(msg, 2)
	end
	opts.ttl_seconds = opts.ttl_seconds or -1

	local fid  = fb:id()
	local name = fb:name()
	local now  = fiber.time()
	local info = {
		fid                = fid;
		name               = name;
		gen                = GENERATION;
		registered         = now;
		csw                = -1;
		csw_upd            = now;
		csw_stuck_seconds  = opts.csw_stuck_seconds or M._.monitor_cfg.csw_stuck_seconds;
		heartrate_seconds  = opts.heartrate_seconds or M._.monitor_cfg.heartrate_seconds;
		heart_upd          = now;
	}
	if opts.ttl_seconds < 0 then
		info.delay_seconds = opts.delay_seconds or M._.monitor_cfg.delay_seconds;
		info.permanent = true
		M._.permanent[fid] = info
	else
		info.ttl_seconds = opts.ttl_seconds
		info.permanent = false
		M._.temporary[fid] = info
	end
end

function M.consider_stable()
	local fibers = M.ps()
	for _, fb in pairs(fibers) do
		if #fb.errors > 0 then
			M._.temporary[fb.fid] = nil
			M._.permanent[fb.fid] = nil
			for k, v in pairs(fb.errors) do
				if v.message == 'fiber_bastard' then
					local msg = ("Allowing bastards and beats from them, fiber=%s"):format(json.encode(fb))
					log.error("FBMONITOR: %s", msg)
					_event("alert", "FBMONITOR_RECONFIGURED", msg)
					M._.monitor_cfg.bastards_allowed       = true
					M._.monitor_cfg.bastards_beats_allowed = true
				end
			end
		end
	end
	return true
end

function M.monitor_info(fb)
	if not fb then
		fb = fiber.self()
	end
	if tonumber(fb) then
		return M._.permanent[fb] or M._.temporary[fb], fiber.find(fb)
	elseif is_fiber(fb) then
		local fid = fb:id()
		return M._.permanent[fid] or M._.temporary[fid], fb
	end

	error("Incorrect usage: either fid or fiber object required", 2)
end

function M.beat(fb)
	local monitor, fbinfo = M.monitor_info(fb)
	if not monitor then
		if not M._.monitor_cfg.bastards_beats_allowed then
			_event("alert", "FBMONITOR_BEAT_UNMONITORED", json.encode(fbinfo))
		end
	else
		monitor.heart_upd = fiber.time()
	end

	return true
end

function M.done(fb)
	local monitor, fbinfo = M.monitor_info(fb)
	if not monitor then
		if not M._.monitor_cfg.bastards_beats_allowed then
			_event("alert", "FBMONITOR_DONE_UNMONITORED", json.encode(fbinfo))
		end
	else
		monitor.fiber_done = fiber.time()
	end
	return true
end

function M.is_bastard_ok(fbinfo)
	if M._.monitor_cfg.bastards_allowed then
		return true
	end
	for _, mask in pairs(M._.monitor_cfg.bastards_masks) do
		if fbinfo.name:match(mask) then
			return true
		end
	end
	return false
end

local function len_fmt(str, len)
	return ("%%-%ds"):format(len):format(str)
end

local legend = {
	{ 'fid',        11 };
	{ 'name',       33 };
	{ 'status',     7  };
	{ 'mon',        6  };
	{ 'gen',        5  };
	{ 'errors',     20 };
}

local legend_str = ''
for _, item in pairs(legend) do
	legend_str = legend_str .. len_fmt(item[1], item[2])
end

function M.human_readable_ps(ps)
	table.sort(ps, function(a, b) return a.name > b.name end)
	local ret = {}
	table.insert(ret, legend_str)
	for _, fbinfo in pairs(ps) do
		local fbline = ''
		for _, item in pairs(legend) do
			local str = ''
			if item[1] == 'errors' then
				for _, err in pairs(fbinfo.errors or {}) do
					str = str .. (err.message:gsub("fiber_", "")) .. ","
				end
				str = str:gsub(",$", "")
				if str == "" then
					str = "-"
				end
			else
				str = tostring(fbinfo[item[1]]) or "-"
			end
			str = len_fmt(str, item[2])
			fbline = fbline .. str
		end
		table.insert(ret, fbline)
	end
	return ret
end

function M.ps(human_readable)
	local fbinfo = fiber.info()

	local res    = {}
	local now    = fiber.time()

	for fid, fb in pairs(fbinfo) do
		local monitor = M.monitor_info(fid)
		local curr = {
			fid        = fid;
			name       = fb.name;
			csw        = fb.csw;
			gen        = -1;
			mon        = not not monitor;
			status     = "alive";
			errors     = {};
		}
		if monitor then
			curr.gen = monitor.gen
			if monitor.csw == fb.csw then
				local stuck_deadline = monitor.csw_upd + monitor.csw_stuck_seconds
				if now > stuck_deadline then
					table.insert(curr.errors, {
						message  = "fiber_stuck";
						deadline = stuck_deadline;
						update   = monitor.csw_upd;
						now      = now;
					})
				end
			else
				monitor.csw     = fb.csw
				monitor.csw_upd = now
			end

			if monitor.heartrate_seconds >= 0 then
				local coma_deadline = monitor.heart_upd + monitor.heartrate_seconds
				if now > coma_deadline then
					table.insert(curr.errors, {
						message  = "fiber_coma";
						deadline = coma_deadline;
						update   = monitor.heart_upd;
						now      = now;
					})
				end
				if monitor.fiber_done then
					table.insert(curr.errors, {
						message = "fiber_zombie";
					})
				end
			end
			if not monitor.permanent then
				local deadline = monitor.registered + monitor.ttl_seconds
				if now > deadline then
					table.insert(curr.errors, {
						message  = "fiber_undead";
						deadline = deadline;
						now      = now;
					})
				end
			end
		else
			if not M.is_bastard_ok(fb) then
				table.insert(curr.errors, {
					message = 'fiber_bastard';
				})
			end
		end
		res[fid] = curr
	end

	for _, monitors in pairs({ M._.permanent, M._.temporary }) do
		for fid, monitor in pairs(monitors) do
			if not fbinfo[fid] then
				local curr = {
					fid        = fid;
					name       = monitor.name;
					csw        = monitor.csw;
					gen        = monitor.gen;
					mon        = true;
					status     = "dead";
					errors     = {};
				}
				if monitor.permanent then
					table.insert(curr.errors, {
						message = "fiber_dead";
					})
				end
				if monitor.heartrate_seconds >= 0 and not monitor.fiber_done then
					table.insert(curr.errors, {
						message = "fiber_crashed";
					})
				end
				if #curr.errors > 0 then
					res[fid] = curr
				else
					if not monitor.permanent and not human_readable then
						monitor.reported = true
					end
				end
			end
		end
	end

	local ret_list = {}
	for _, info in pairs(res) do
		table.insert(ret_list, info)
	end

	if human_readable then
		return M.human_readable_ps(ret_list)
	else
		return ret_list
	end
end

function M.on_event(cb)
	M._.on_event = cb
end

local old_fibers = {}
for fid, v in pairs(permanent) do
	if v.gen ~= GENERATION then
		table.insert(old_fibers, fid)
	end
end

for _, fid in pairs(old_fibers) do
	local monitor = permanent[fid]
	M._.temporary[fid] = {
		fid                = fid;
		name               = monitor.name;
		gen                = monitor.gen;
		registered         = fiber.time();
		ttl_seconds        = monitor.delay_seconds or M._.monitor_cfg.delay_seconds;
		csw                = monitor.csw;
		csw_upd            = monitor.csw_upd;
		csw_stuck_seconds  = monitor.csw_stuck_seconds;
		heartrate_seconds  = monitor.heartrate_seconds;
		heart_upd          = monitor.heart_upd;
	}
	permanent[fid] = nil
end

fiber.create(function()
	fiber.yield()
	local me = fiber.self()
	me:name(GENERATION .. ":fmonitor")
	M.monitor({
		delay     = M._.config.period + 100;
		heartrate = M._.config.period + 100;
	})
	while GENERATION == package.reload.count do
		M.beat()
		local ok, err = pcall(function()
			local fid_delete = {}
			for fid, monitor in pairs(M._.temporary) do
				fiber.yield()
				local fb = fiber.find(fid)
				if not fb then
					if monitor.reported then
						if monitor.heartrate_seconds < 0 then
							table.insert(fid_delete, fid)
						elseif monitor.heartrate_seconds >= 0 and monitor.fiber_done then
							table.insert(fid_delete, fid)
						end
					end
				end
			end

			for _, fid in pairs(fid_delete) do
				M._.temporary[fid] = nil
			end
		end)
		if not ok then
			_event('alert', "FBMONITOR_FIBER_FAILED", json.encode(err))
		end
		fiber.sleep(M._.config.period)
	end
	M.done()
end)

fiber.create(function()
	fiber.name(GENERATION .. ':watchdog')
	local last = fiber.time()
	while GENERATION == package.reload.count and M._.monitor_cfg.watchdog_period > 0 do
		fiber.sleep(M._.monitor_cfg.watchdog_period)
		local now = fiber.time()
		local loop_time = now - last
		if loop_time > M._.monitor_cfg.watchdog_lag then
			log.info("Loop take too long: %0.2f instead of %s", now - last, M._.monitor_cfg.watchdog_lag)
		end
		_event("loop_time", loop_time)
		last=now
	end
end)

return M

package = 'monitoring-fiber'
version = 'scm-1'

source  = {
	url    = 'git://github.com/moonlibs/monitoring-fiber.git',
	branch = 'master',
}

description = {
	summary  = "Package for monitoring fibers' health",
	homepage = 'https://github.com/moonlibs/monitoring-fiber.git',
	license  = 'BSD',
}

dependencies = {
	"lua >= 5.1",
	"package-reload",
	"stash",
	"val"
}

build = {
	type = 'builtin',
	modules = {
		['monitoring.fiber'] = 'monitoring/fiber.lua'
	}
}

-- vim: syntax=lua

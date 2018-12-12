import socket
import cmd

argsByApp = {
	'PacketCount': '[interval in seconds]',
	'ByteCount': '[interval in seconds]',
	'PacketLog': ''
}

def intIfNumeric(str):
	try:
		return int(str)
	except Exception as e:
		return str

class ControllerCLI(cmd.Cmd):
	intro = 'Welcome to the controller CLI.\nType help or ? to list commands.\n'
	prompt = 'controller> '

	def __init__(self, sock):
		cmd.Cmd.__init__(self)
		self.sock = sock

	def do_list(self, args):
		'Usage: list\nLists all the available apps.\n'

		print 'List of available apps and their usage:\n'
		for app, appArgs in argsByApp.items():
			print '%s - install %s %s' % (app, app, appArgs)

		print ''

	def do_install(self, args):
		'Usage: install [app] [args...]\nFor list of available apps run list command.\n'

		args = args.split(' ')

		if len(args) < 1:
			print 'App to install not specified'
		elif not argsByApp.has_key(args[0]):
			print '%s is not a supported app' % (args[0],)
		else:
			self.sock.send("('install', %s ,%s)" % (args[0], [intIfNumeric(arg) for arg in args[1:]]))

	def do_uninstall(self, args):
		'Usage: uninstall [app]\n'

		args = args.split(' ')

		if len(args) < 1:
			print 'App to uninstall not specified'
		elif not argsByApp.has_key(args[0]):
			print '%s is not a supported app' % (args[0],)
		else:
			self.sock.send("('uninstall', %s ,[])" % (args[0]))

	def do_EOF(self, args):
		'Usage: Ctrl+D exits the CLI\n'

		self.sock.close()

		print ''
		return True


s = socket.socket()
s.connect((socket.gethostname(), 9999))

s.send("('install', LearningSwitches, [])")

ControllerCLI(s).cmdloop()

import socket

from ryu.base.app_manager import RyuApp, AppManager
from ryu.app import wsgi
from ryu.lib import hub
from ryu.controller.ofp_handler import OFPHandler
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls

from apps.learning_switches import LearningSwitches
from apps.byte_counter import ByteCount
from apps.packet_counter import PacketCount
from apps.packet_logger import PacketLog
from apps.switch_listener import SwitchListen


class Loader(RyuApp):
	def __init__(self, *args, **kwargs):
		super(Loader, self).__init__(*args, **kwargs)

		self.feature_events = []
		self.table_id = 0

		self.ryu_mgr = AppManager.get_instance()
		self.install(OFPHandler)
		self.install(SwitchListen(lambda ev: self.switch_features_handler(ev)))
		self.start_listening()

	@set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
	def switch_features_handler(self, ev):
		self.feature_events.append(ev)

	def create_context(self, key, cls):
		context = None

		if issubclass(cls, RyuApp):
			context = self.ryu_mgr._instantiate(None, cls)
		else:
			context = cls()

		if key in self.ryu_mgr.contexts:
			return None

		self.ryu_mgr.contexts.setdefault(key, context)

		return context


	def install(self, app_cls):
		app_contexts = app_cls._CONTEXTS

		new_contexts = []
		for k in app_contexts:
			context_cls = app_contexts[k]
			ctx = create_context(k, context_cls)

			if ctx and issubclass(context_cls, RyuApp):
				new_contexts.append(ctx)

		app = self.ryu_mgr.instantiate(app_cls, **self.ryu_mgr.contexts)
		new_contexts.append(app)

		for ctx in new_contexts:
			ctx.start()

	def uninstall(self, app_cls):
		app = None
		for _app in self.ryu_mgr.applications.values():
			if _app.__module__ == app_cls.__module__ and _app.__class__.__name__ == app_cls.__name__:
				app = _app
				break
		else:
			return

		self.ryu_mgr.uninstantiate(app.name)
		app.stop()

		app_contexts = app_cls._CONTEXTS

		ctxs_to_keep = []
		for app_cls in [app.__class__ for app in self.ryu_mgr.applications.values()]:
			ctxs_to_keep.extend(app_cls._CONTEXTS.keys())

		for ctx_to_remove in [ctx for ctx in app_contexts if ctx not in ctxs_to_keep]:
			ctx_cls = app_contexts[ctx_to_remove]
			ctx = self.ryu_mgr.contexts[ctx_to_remove]

			if issubclass(ctx_cls, RyuApp):
				ctx.stop()

			if ctx.name in self.ryu_mgr.applications:
				del self.ryu_mgr.applications[ctx.name]

			if ctx_to_remove in self.ryu_mgr.contexts:
				del self.ryu_mgr.contexts[ctx_to_remove]

			if ctx.name in app_manager.SERVICE_BRICKS:
				del app_manager.SERVICE_BRICKS[ctx.name]

	def start_listening(self):
		listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		listener.bind((socket.gethostname(), 9999))
		listener.listen(1)

		while True:
			client,_ = listener.accept()

			while True:
				msg = client.recv(1024).strip()
				if not msg:
					break

				operation, cls, params = eval(msg)

				if operation == 'install':
					params = [self.feature_events, self.table_id] + params

					self.install(cls(*params))
					self.table_id += 1
				else:
					self.uninstall(cls())
					self.table_id -= 1

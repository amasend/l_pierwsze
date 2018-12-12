from ryu.app.wsgi import ControllerBase, Response, route


route_name = 'stats_api'
rest_api_root = '/api'

class StatsRestApi(ControllerBase):

    controller_instance_name = 'controller_main'
    dpset_instance_name = 'controller_dpset'

    def __init__(self, req, link, data, **config):

        super(StatsRestApi, self).__init__(req, link, data, **config)

        # setup dpset
        self.dpset_instance = data[self.dpset_instance_name]

        # setup controller instance access
        self.controller_instance = data[self.controller_instance_name]  # running controller instance
        self.logger = self.controller_instance.logger  # output logger

    @route(route_name, rest_api_root + '/hello', methods=['GET'])
    def hello_world(self, req, **_kwargs):

        # test method
        response = {'msg': 'Hello, world!'}
        return Response(status=200, json_body=response)

    @route(route_name, rest_api_root + '/interval', methods=['GET'])
    def get_stats_interval(self, req, **kwargs):
        interval = self.controller_instance.interval
        return Response(status=200, json_body={'kwargs': kwargs, 'interval': interval})

    @route(route_name, rest_api_root + '/interval', methods=['POST','PUT'])
    def set_stats_interval(self, req, **kwargs):
        try:
            data = req.json if req.body else {}
        except ValueError:
            raise Response(status=400)

        try:
            self.controller_instance.set_interval(int(data['interval']))
            self.logger.info(data)
        except Exception as e:
            return Response(status=500)
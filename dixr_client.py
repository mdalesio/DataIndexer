from p4p.rpc import rpcproxy, rpccall
@rpcproxy
class MyProxy(object):
    @rpccall('%sget_header_given_uid')
    def get_header_given_uid(uid='s'):
        pass

from p4p.client.thread import Context
ctxt = Context('pva')
proxy = MyProxy(context=ctxt, format='pv:call:')
print(proxy.get_header_given_uid(uid='ddbdc90e-6d75-47e4-a71c-abb8bb30f000'))

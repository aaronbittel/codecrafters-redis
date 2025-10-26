# from app.client import Client
#
#
# def test_xrange_query(client: Client):
#     client.xadd(
#         key="somekey", id="1526985054069-0", values={"temperature": 36, "humidity": 95}
#     )
#     client.xadd(
#         key="somekey", id="1526985054079-0", values={"temperature": 37, "humidity": 94}
#     )
#     client.xrange(key="somekey", start="1526985054069" stop="1526985054079")

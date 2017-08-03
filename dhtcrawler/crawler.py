from maga import Maga
from mala import get_metadata
import logging
import pymongo
import json

logging.basicConfig(level=logging.INFO)
WORKING_INFOHASHES = set()


class Crawler(Maga):

  def __init__(self, config):
    super(Crawler, self).__init__()

    client = pymongo.MongoClient(config["mongo_host"], config["mongo_port"])
    print("connect to mongodb: {}:{}".format(config["mongo_host"], config["mongo_port"]))
    self.db = client[config["mongo_dbname"]]

  # async def handle_get_peers(self, infohash, addr):
  #   logging.info(
  #     "Receive get peers message from DHT {}. Infohash: {}.".format(
  #       addr, infohash
  #     )
  #   )
  async def handle_get_peers(self, infohash, addr):
    pass

  async def handle_announce_peer(self, infohash, addr, peer_addr):
    if infohash in WORKING_INFOHASHES:
      return

    logging.info(
      "Receive announce peer message from DHT {}. Infohash: {}. Peer address:{}".format(
          addr, infohash, peer_addr))

    WORKING_INFOHASHES.add(infohash)
    # logging.info("See new infohash: " + infohash)
    metainfo = await get_metadata(
        infohash, peer_addr[0], peer_addr[1], loop=self.loop
    )
    WORKING_INFOHASHES.discard(infohash)

    if metainfo:
        print(" >>>> ---- should save meta info")
        print(metainfo)
        # await save_torrent_info(infohash, metainfo)


if __name__ == "__main__":
  with open("config.json") as fin:
    config = json.loads(fin.read())

  crawler = Crawler(config)
  # Set port to 0 will use a random available port
  crawler.run(port=0)
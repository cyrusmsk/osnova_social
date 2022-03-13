import json
import time
import logging
from datetime import datetime
from dataclasses import dataclass

from src.config_loader import ConfigLoader
from src.ochoba_api_wrapper import OchobaApiWrapper

TJ_POSTS_DIR = "/home/cyrus/Personal/blog/osnova_social/data/tj_data/posts"
VC_POSTS_DIR = "/home/cyrus/Personal/blog/osnova_social/data/vc_data/posts"
DTF_POSTS_DIR = "/home/cyrus/Personal/blog/osnova_social/data/dtf_data/posts"


logging.basicConfig(filename='posts.log', encoding='utf-8', level=logging.WARNING)

class GetPosts:
    @dataclass
    class Stats:
        request_count: int = 0
        post_count: int = 0
        error_count: int = 0
        requests_since_last_429: int = 0
        requests_since_last_500: int = 0

    def __init__(self):
        config = ConfigLoader.load()
        self.api = OchobaApiWrapper(config["api"])
        self.stats = self.Stats()

    def get_posts(self):
        print(f"Started at {datetime.now().strftime('%H:%M:%S')}")
        logging.info(f"Started at {datetime.now().strftime('%H:%M:%S')}")

        for post_id in range(1050000, 1120000):
            try:
                if self.stats.request_count % 100 == 0:
                    print(
                        "{0}: {1} requests processed ({2} posts, {3} errors)".format(
                            datetime.now().strftime("%H:%M:%S"),
                            self.stats.request_count,
                            self.stats.post_count,
                            self.stats.error_count,
                        )
                    )
                    logging.debug(
                        "{0}: {1} requests processed ({2} posts, {3} errors)".format(
                            datetime.now().strftime("%H:%M:%S"),
                            self.stats.request_count,
                            self.stats.post_count,
                            self.stats.error_count,
                        )
                    )
                if self.stats.request_count % 3 == 0:
                    time.sleep(1)
                self.__get_post(post_id)

            except Exception:
                print("Exception! Post_id:" + str(post_id))
                logging.warning(f"Exception has raised while processing post_id: {post_id}")
                continue

        print("Everything is finished!")


    def add_nulls(self, post_id, numcount=10):
        len_number = len(str(post_id))
        if len_number > 10:
            raise ValueError
        return "0" * (numcount - len_number) + str(post_id)

    def __get_post(self, post_id):
        response = self.api.execute("entry/" + str(post_id))
        if response.status_code == 429:
            # Too Many Requests
            print(
                datetime.now().strftime("%H:%M:%S")
                + ": 429 Too Many Requests. Requests processed since last 429 error: "
                + str(self.stats.requests_since_last_429)
                + ". Wait for 60 seconds and repeat"
            )
            logging.warning(
                datetime.now().strftime("%H:%M:%S")
                + ": 429 Too Many Requests. Requests processed since last 429 error: "
                + str(self.stats.requests_since_last_429)
                + ". Wait for 60 seconds and repeat"
            )
            self.stats.requests_since_last_429 = 0
            time.sleep(60)
            self.__get_post(post_id)
            return
        if response.status_code == 500:
            # Internal Server Error
            print(
                datetime.now().strftime("%H:%M:%S")
                + ": 500 Internal Server Error. Requests processed since last 500 error: "
                + str(self.stats.requests_since_last_500)
                + ". Wait for 60 seconds and repeat"
            )
            logging.warning(
                datetime.now().strftime("%H:%M:%S")
                + ": 500 Internal Server Error. Requests processed since last 500 error: "
                + str(self.stats.requests_since_last_500)
            )
            return

        response_json = response.json()

        number_with_nulls = self.add_nulls(post_id)
        output_file = DTF_POSTS_DIR + "/" + number_with_nulls

        if "error" in response_json:
            self.stats.error_count += 1
        else:
            with open(output_file, "w", encoding="utf-8") as file:
                file.write(json.dumps(response_json))
            self.stats.post_count += 1

        self.stats.request_count += 1
        self.stats.requests_since_last_429 += 1
        self.stats.requests_since_last_500 += 1


if __name__ == "__main__":
    GetPosts().get_posts()

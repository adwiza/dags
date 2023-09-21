import os
from anyio import sleep

from rd_chassis.misc.helpers import epoch_millis_to_utc
from rd_chassis.tasks.base_task import BaseTask

from gateways.bi_es import BiEs
from gateways.bi_clickhouse import BiClickhouse


class GetNewDocs(BaseTask):
    def __init__(self, docs_send_stream, index):
        super().__init__()
        self.docs_send_stream = docs_send_stream
        self.chunk_size = int(os.getenv("DOCS_CHUNK_SIZE"))
        self.index = index
        self.table_name = str(index).replace("-", "_")

    async def _start(self) -> None:
        last_origin_processed_at = await self.get_last_origin_processed_at()

        while True:
            docs = await BiEs().get_new_docs(self.index, last_origin_processed_at, self.chunk_size)
            self.logger.debug(f"{self.index} got {len(docs)} docs")
            if docs:
                await self.docs_send_stream.send(docs)
                last_origin_processed_at = docs[-1]["meta"]["origin_processed_at"]
            self.logger.debug(f"{self.index} "
                              f"last_origin_processed_at = {epoch_millis_to_utc(last_origin_processed_at)}")
            if len(docs) < self.chunk_size:
                self.logger.debug(f"{self.index} Sleeping...")
                await sleep(15)

    async def get_last_origin_processed_at(self) -> int | None:
        last_origin_processed_at = await BiClickhouse().get_last_origin_processed_at(self.table_name)
        if last_origin_processed_at:
            self.logger.debug(f"{self.index} "
                              f"last_origin_processed_at = {epoch_millis_to_utc(last_origin_processed_at)}")
            return last_origin_processed_at - 60 * 1000  # a bit of overlap just in case
        else:
            self.logger.debug(f"{self.index} last_origin_processed_at not found")

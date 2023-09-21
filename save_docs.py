import os
from anyio import WouldBlock, sleep
from flatten_dict import flatten

from rd_chassis.misc.helpers import now_epoch_millis
from rd_chassis.tasks.base_task import BaseTask

from gateways.bi_clickhouse import BiClickhouse
from misc.helpers import get_columns


class SaveDocs(BaseTask):
    def __init__(self, docs_recv_stream, index):
        super().__init__()
        self.docs_recv_stream = docs_recv_stream
        self.flush_interval = int(os.getenv("DOCS_FLUSH_INTERVAL"))
        self.flush_size = int(os.getenv("DOCS_FLUSH_SIZE"))
        self.table_name = str(index).replace("-", "_")

    async def _start(self) -> None:
        docs_for_save = []
        last_timestamp = now_epoch_millis()
        while True:
            try:
                chunk = self.docs_recv_stream.receive_nowait()
                docs_for_save.extend(chunk)
                delta = now_epoch_millis() - last_timestamp
                if docs_for_save and (
                    (delta > self.flush_interval) or (len(docs_for_save) > self.flush_size)
                ):

                    self.logger.debug(
                        f"{self.table_name} go to saving. "
                        f"After time: {delta // 1000:.2f}sec. "
                        f"from last save, and len(docs_for_save) = {len(docs_for_save)}"
                    )

                    columns_name, values_tuples = await self.docs_unpack(docs_for_save)
                    await BiClickhouse().insert_many(self.table_name, columns_name, values_tuples)
                    docs_for_save.clear()
                    last_timestamp = now_epoch_millis()

            except WouldBlock:
                await sleep(1)
                pass

    async def docs_unpack(self, docs):
        columns = get_columns(self.table_name)
        columns_name = [col.name for col in columns]

        values_tuples = []
        for doc in docs:
            row = []
            doc = flatten(doc, reducer='dot')
            for col in columns:
                key = col.name
                row.append(col.get_value(doc.get(key, None)))
            values_tuples.append(tuple(row))

        return columns_name, values_tuples

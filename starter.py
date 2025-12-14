from scrapper import producer
from scrapper import stop_queue

event = {
        "task":"hello_world"
}
group_id = "asia"

producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))

producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
producer.apply_async((event, group_id))
# stop_queue.apply_async()
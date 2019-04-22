"""
This file is regarding the publishing the streaming data into pubsub:

Sensor data from the lane sensors set up in the city's
Each sensors records the speed of car riding in particular lane

"""

import timeit,gzip
from google.cloud import pubsub_v1

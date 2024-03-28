
##################################################################################################################

"""
Parent class for the CAF framework. To use, the following must be defined in the child class:
MAF:
    Optional:
    - on_set_state

CAF:
    - message_to_publish(self) (property)
    - process_msg(self, msg)
    - next_state(self) (property)
    # - action_to_take(self) (property)
    # - process_step(self, obs, reward, done, infos)

    Optional:
    - on_new_task(self, task_id, task_data)
"""

# Built-in/Generic Imports
import os
from abc import abstractmethod
from typing import List, Optional
from datetime import datetime, timedelta
from random import randint
import numpy as np
import pandas as pd
from json import dumps, loads
from pprint import pprint, pformat
import sys

# Libs
# ROS2 Imports
import rclpy
from rclpy.node import Node
from rclpy.time import Time
from geometry_msgs.msg import Twist, PoseStamped, Point
from rclpy.qos import QoSProfile, QoSReliabilityPolicy, QoSHistoryPolicy

# TODO: Cleanup
# NetworkX
import networkx as nx

# Local Imports
from orchestra_config.orchestra_config import *     # KEEP THIS LINE, DO NOT REMOVE
from orchestra_config.sim_config import *
from maaf_msgs.msg import TeamCommStamped, Bid, Allocation

from maaf_tools.datastructures.task.Task import Task
from maaf_tools.datastructures.task.TaskLog import TaskLog

from maaf_tools.datastructures.agent.Agent import Agent
from maaf_tools.datastructures.agent.Fleet import Fleet
from maaf_tools.datastructures.agent.AgentState import AgentState

from maaf_tools.tools import *

##################################################################################################################


class Controller_graph(Node):
    def __init__(self):
        # ----------------------------------- Node Configuration
        Node.__init__(
            self,
            node_name="Controller_graph",
        )

        self.fleet = Fleet()

        # ---------------------------------- Subscribers
        # ---------- simulator_signals
        self.simulator_signals_sub = self.create_subscription(
            msg_type=TeamCommStamped,
            topic=topic_simulator_signals,
            callback=self.simulator_signals_callback,
            qos_profile=qos_simulator_signals
        )
        # ---------- environment
        self.env = None

        self.env_sub = self.create_subscription(
            msg_type=TeamCommStamped,
            topic=topic_environment,
            callback=self.env_callback,
            qos_profile=qos_env
        )

        # ---------- epoch
        self.sim_epoch_sub = self.create_subscription(
            msg_type=TeamCommStamped,
            topic=topic_epoch,
            callback=self.sim_epoch_callback,
            qos_profile=qos_sim_epoch
        )

        # ---------- goal
        self.goals_sub = self.create_subscription(
            msg_type=TeamCommStamped,
            topic=topic_goals,
            callback=self.goal_callback,
            qos_profile=qos_goal
        )

        # ---------------------------------- Publishers
        # ---------- events_instructions
        self.sim_events_instructions_pub = self.create_publisher(
            msg_type=TeamCommStamped,
            topic=topic_sim_events_instructions,
            qos_profile=qos_sim_events_instructions
        )

        # ---------- tasks
        self.tasks_pub = self.create_publisher(
            msg_type=TeamCommStamped,
            topic=topic_tasks,
            qos_profile=qos_tasks
        )

        # ---------- /robot_.../pose
        self.robot_pose_pub = {}

        # ---------------------------------- Timer
        # pose_emission_interval = .1
        # self.pose_pub_timer = self.create_timer(
        #     timer_period_sec=pose_emission_interval,
        #     callback=self.pose_publisher_timer_callback
        # )

        self.get_logger().info("Controller_graph node has been initialized")

    @property
    def current_timestamp(self) -> float:
        """
        Get current timestamp as float value in seconds

        :return: timestamp float in seconds
        """
        # -> Get current ROS time as timestamp
        time_obj = self.get_clock().now().to_msg()

        return timestamp_from_ros_time(time_obj)

    @property
    def current_allocation(self):
        allocations = []

        for agent in self.fleet:
            if agent.local["goal"] is not None:
                allocations.append(agent.local["goal"])

        return allocations

    def env_callback(self, msg: TeamCommStamped):
        data = loads(msg.memo)
        self.env = {
            "graph": nx.node_link_graph(data["graph"]),
            "pos": {eval(k): v for k, v in data["pos"].items()}
        }

    def goal_callback(self, msg: TeamCommStamped):
        """
        Callback for the goal subscription.
        """

        msg_memo = loads(msg.memo)

        # -> Construct source agent
        source_agent = Agent.from_dict(msg_memo["agent"])

        # -> If the agent is not in the fleet, add it
        if msg.source not in self.fleet.ids:
            # -> Create pos publisher
            qos_pose = QoSProfile(
                reliability=QoSReliabilityPolicy.BEST_EFFORT,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1
            )

            self.robot_pose_pub[msg.source] = self.create_publisher(
                msg_type=PoseStamped,
                topic=f"/{msg.source}{topic_pose}",
                qos_profile=qos_pose
            )

            self.fleet.add_agent(agent=source_agent)
            self.fleet[msg.source].local["tasks"] = {}

        if msg.meta_action == "update":
            self.fleet[msg.source].plan = source_agent.plan
            self.fleet[msg.source].local["tasks"] = {}

            for task_id, task_dict in msg_memo["tasks"].items():
                self.fleet[msg.source].local["tasks"][task_id] = Task.from_dict(task_dict)

            self.get_logger().info(f"Agent {msg.source} updated its plan")
            self.get_logger().info(f"{pformat(self.fleet[msg.source].plan.path)}")

    def sim_epoch_callback(self, msg: TeamCommStamped):
        """
        Step the simulation to the next epoch.
        """

        # -> Load the message
        memo = loads(msg.memo)

        # ... for all agents
        for agent in self.fleet:
            if agent.plan.task_bundle:
                current_task_id = agent.plan.task_bundle[0]

                if len(agent.plan.paths[current_task_id]["path"]) > 1:
                    # -> Get next pose in path
                    new_state = agent.plan.paths[current_task_id]["path"][0]

                    # -> Update agent state
                    agent.state.x = new_state[0]
                    agent.state.y = new_state[1]

                    self.get_logger().info(f"Epoch {memo['epoch']}: Agent {agent.id} moved to {new_state}")
                    agent.plan.paths[current_task_id]["path"].pop(0)
                else:
                    task = agent.local["tasks"][current_task_id]

                    # -> Edit the task termination
                    task.termination_timestamp = self.current_timestamp
                    task.termination_source_id = agent.id
                    task.status = "completed"

                    # -> Send completion message
                    msg = TeamCommStamped()

                    msg.source = "controller_graph"
                    msg.target = agent.id
                    msg.meta_action = "completed"
                    msg.memo = dumps(task.asdict())

                    self.sim_events_instructions_pub.publish(msg)

                    # -> Remove the task from the plan
                    self.fleet[agent.id].plan.remove_task(current_task_id)
                    del self.fleet[agent.id].local["tasks"][current_task_id]

        self.pose_publisher_timer_callback()

    def pose_publisher_timer_callback(self):
        """
        Publish the pose of all agents.
        """

        # ... for all agents
        for agent in self.fleet:
            x, y = agent.state.x, agent.state.y

            # -> Publish new pose
            msg = PoseStamped()
            msg.header.stamp = self.get_clock().now().to_msg()
            msg.header.frame_id = "map"
            msg.pose.position = Point(x=float(x), y=float(y), z=0.)

            self.robot_pose_pub[agent.id].publish(msg)

    def simulator_signals_callback(self, msg: TeamCommStamped):
        if msg.meta_action == "order 66":
            self.get_logger().info("Received order 66: Terminating simulation")

            # -> Terminate node
            self.destroy_node()

            # -> Terminate script
            sys.exit()


def main(args=None):
    # `rclpy` library is initialized
    rclpy.init(args=args)

    path_sequence = Controller_graph()

    rclpy.spin(path_sequence)

    path_sequence.destroy_node()
    rclpy.shutdown()

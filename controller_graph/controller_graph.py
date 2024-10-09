
##################################################################################################################
"""
Executive layer simulant.
> Simulates the movement of robots in the environment.
> Simulate the generation of completion signals for tasks.
"""

# Built-in/Generic Imports
import os
from abc import abstractmethod
from typing import List, Optional
from datetime import datetime, timedelta
from random import randint
import numpy as np
import pandas as pd
from pprint import pprint, pformat
import sys
from copy import deepcopy

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
try:
    from orchestra_config.orchestra_config import *     # KEEP THIS LINE, DO NOT REMOVE
    from orchestra_config.sim_config import *
    from maaf_msgs.msg import TeamCommStamped, Bid, Allocation

    from maaf_tools.datastructures.task.Task import Task
    from maaf_tools.datastructures.task.TaskLog import TaskLog

    from maaf_tools.datastructures.agent.Agent import Agent
    from maaf_tools.datastructures.agent.Fleet import Fleet
    from maaf_tools.datastructures.agent.AgentState import AgentState

    from maaf_tools.tools import *

except ImportError:
    from orchestra_config.orchestra_config.orchestra_config import *  # KEEP THIS LINE, DO NOT REMOVE
    from orchestra_config.orchestra_config.sim_config import *
    from maaf_msgs.msg import TeamCommStamped, Bid, Allocation

    from maaf_tools.maaf_tools.datastructures.task.Task import Task
    from maaf_tools.maaf_tools.datastructures.task.TaskLog import TaskLog

    from maaf_tools.maaf_tools.datastructures.agent.Agent import Agent
    from maaf_tools.maaf_tools.datastructures.agent.Fleet import Fleet
    from maaf_tools.maaf_tools.datastructures.agent.AgentState import AgentState

    from maaf_tools.maaf_tools.tools import *


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
        self.environment = None

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
        self.environment = {
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

        # -> Set paths property manually
        if "paths" in msg_memo["agent"]["plan"].keys():
            source_agent.plan.paths = msg_memo["agent"]["plan"]["paths"]
        else:
            source_agent.plan.paths = None

        # -> If the agent is not in the fleet, add it
        if msg.source not in self.fleet.ids:
            self.robot_pose_pub[msg.source] = self.create_publisher(
                msg_type=PoseStamped,
                topic=f"/{msg.source}{topic_pose}",
                qos_profile=qos_pose
            )

            self.fleet.add_agent(agent=source_agent)
            self.fleet[msg.source].local["tasks"] = {}

        if msg.meta_action == "update":
            # # -> Verify start of new plan is the same as the current agent pose
            # if "traceback" in msg_memo["kwargs"].keys():
            #     self.get_logger().info(f"{msg_memo['kwargs']['traceback']} - Agent {msg.source} at {self.agent_pos(msg.source)} updating its plan: {source_agent.plan.path}")
            # else:
            #     self.get_logger().info(f"??? - Agent {msg.source} at {self.agent_pos(msg.source)} updating its plan: {source_agent.plan.path}")

            # self.get_logger().info(f"{msg.source}: {source_agent.plan}")

            self.fleet[msg.source].plan = source_agent.plan
            self.fleet[msg.source].local["tasks"] = {}

            for task_id, task_dict in msg_memo["tasks"].items():
                self.fleet[msg.source].local["tasks"][task_id] = Task.from_dict(task_dict)

    def sim_epoch_callback(self, msg: TeamCommStamped):
        """
        Step the simulation to the next epoch.
        """

        # -> Load the message
        memo = loads(msg.memo)

        msg_backlog = []

        move_logs = "States logs:"
        task_completed_logs = "Tasks completion log"

        # ... for all agents
        for agent in self.fleet:
            if agent.plan.task_sequence:
                current_task_id = agent.plan.task_sequence[0]

                # self.get_logger().info(f"Agent {agent.id} at {agent.state.pos} - Task {current_task_id} at {agent.plan.paths[current_task_id]['path']}")

                if len(agent.plan.paths[current_task_id]["path"]) > 1:
                    # self.get_logger().info(f"Agent {agent.id} at {agent.state.x, agent.state.y} ({agent.plan.paths[current_task_id]['path']})")

                    # if agent.plan.paths[current_task_id]["path"][0] != agent.state.pos:
                    assert agent.plan.paths[current_task_id]["path"][0] == agent.state.pos # -> Check if agent is at the start of the path

                    # -> Get next pose in path
                    agent.plan.paths[current_task_id]["path"].pop(0)
                    new_state = agent.plan.paths[current_task_id]["path"][0]

                    # -> Update agent state
                    agent.state.x = new_state[0]
                    agent.state.y = new_state[1]

                    move_logs += f"\n{agent.id} > moved to {new_state}"

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

                    # self.sim_events_instructions_pub.publish(msg)
                    msg_backlog.append(deepcopy(msg))

                    task_completed_logs += f"\n{agent.id} v Task {current_task_id} at {agent.state.pos} completed"

                    # -> Remove the task from the plan
                    del self.fleet[agent.id].local["tasks"][current_task_id]
                    self.fleet[agent.id].plan.remove_task(current_task_id)

        # -> Print logs
        self.get_logger().info(move_logs)
        # self.get_logger().info(task_completed_logs)

        # -> Publish new poses
        self.pose_publisher_timer_callback()

        # -> Publish completion msgs
        for msg in msg_backlog:
            self.sim_events_instructions_pub.publish(msg)

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

#!/usr/bin/env python3
"""
Simplified Coordinator with Primary-Backup Fault Tolerance
"""

import grpc
from concurrent import futures
import time
import threading
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

# Global node registry dict
node_registry = {}
registry_lock = threading.Lock()

# Fault tolerance: task replicas mapping
task_replicas = {}
replicas_lock = threading.Lock()

from config import COORDINATOR_PORT
import compute_pb2
import compute_pb2_grpc


class SimpleCoordinator(compute_pb2_grpc.CoordinatorServicer):
    def __init__(self, my_id, my_port):
        self.my_id = my_id
        self.my_port = my_port

        self.node_registry = node_registry
        self.registry_lock = registry_lock

        self.task_replicas = task_replicas
        self.replicas_lock = replicas_lock

        self.heartbeat_threads = {}
        self.is_leader = True

    def RegisterNode(self, request, context):
        """Handle node registration"""
        node_id = request.node_id
        node_data = {
            "node_id": node_id,
            "address": f"{request.ip}:{request.port}",
            "ip": request.ip,
            "port": request.port,
            "cpu_cores": request.cpu_cores,
            "memory_gb": request.memory_gb,
            "status": "available",
            "last_seen": time.time(),
        }

        with self.registry_lock:
            self.node_registry[node_id] = node_data

        return compute_pb2.RegisterResponse(
            status="success", message=f"Node {node_id} registered"
        )

    def GetAvailableNodes(self, request, context):
        """Return available nodes"""
        requester = request.requesting_node_id
        num_requested = request.num_nodes

        with self.registry_lock:
            available = [
                compute_pb2.NodeInfo(
                    node_id=info["node_id"],
                    address=info["address"],
                    cpu_cores=info["cpu_cores"],
                    memory_gb=info["memory_gb"],
                    status=info["status"],
                )
                for node_id, info in self.node_registry.items()
                if node_id != requester and info["status"] == "available"
            ]

        return compute_pb2.NodeListResponse(nodes=available[:num_requested])

    def UpdateNodeStatus(self, request, context):
        """Update node status"""
        node_id = request.node_id
        with self.registry_lock:
            if node_id in self.node_registry:
                self.node_registry[node_id]["status"] = request.status
                self.node_registry[node_id]["last_seen"] = time.time()

        return compute_pb2.StatusResponse(status="success")

    def RegisterTaskReplica(self, request, context):
        """Register a task with primary-backup replication"""
        task_id = request.task_id
        primary = request.primary_node_id
        backup = request.backup_node_id

        with self.replicas_lock:
            self.task_replicas[task_id] = {
                "primary": primary,
                "backup": backup,
                "data": request.task_data,
                "created_at": time.time(),
                "status": "active",
            }

        return compute_pb2.TaskReplicaResponse(
            success=True,
            message=f"Task {task_id} registered with primary-backup replication",
        )

    def GetBackupForTask(self, request, context):
        """Get backup node and data for a task"""
        task_id = request.task_id

        with self.replicas_lock:
            if task_id not in self.task_replicas:
                return compute_pb2.TaskBackupResponse(
                    backup_node_id="", task_data=b"", has_backup=False
                )

            replica = self.task_replicas[task_id]
            backup_node = replica["backup"]
            task_data = replica["data"]

        return compute_pb2.TaskBackupResponse(
            backup_node_id=backup_node, task_data=task_data, has_backup=True
        )

    def PromoteBackupToPrimary(self, request, context):
        """Promote backup to primary when primary fails"""
        task_id = request.task_id
        failed_primary = request.failed_primary

        with self.replicas_lock:
            if task_id not in self.task_replicas:
                return compute_pb2.PromotionResponse(
                    success=False,
                    promoted_backup="",
                    message=f"Task {task_id} not found",
                )

            replica = self.task_replicas[task_id]
            if replica["primary"] != failed_primary:
                return compute_pb2.PromotionResponse(
                    success=False,
                    promoted_backup="",
                    message=f"Primary mismatch for {task_id}",
                )

            promoted_backup = replica["backup"]
            replica["primary"] = promoted_backup
            replica["backup"] = None

        return compute_pb2.PromotionResponse(
            success=True,
            promoted_backup=promoted_backup,
            message=f"Backup {promoted_backup} promoted to primary for {task_id}",
        )


def serve(my_id, my_port):
    """Start coordinator server"""
    coordinator = SimpleCoordinator(my_id, my_port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    compute_pb2_grpc.add_CoordinatorServicer_to_server(coordinator, server)
    server.add_insecure_port(f"0.0.0.0:{my_port}")
    server.start()

    print(f"Coordinator started on port {my_port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    serve(1, COORDINATOR_PORT)

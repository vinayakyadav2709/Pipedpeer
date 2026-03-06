"""
Network management utilities
Handles P2P communication, availability checks, and task dispatch
"""

import grpc.aio
import pickle
import time
import asyncio
import sys
import os

# Fix imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import *
import compute_pb2
import compute_pb2_grpc


class NetworkManager:
    """Handles all network operations for a node"""

    def __init__(self, node, latency_simulation=0.0):
        self.node = node
        self.latency_simulation = latency_simulation

    async def register_with_coordinator(self):
        """Register node with coordinator"""
        try:
            async with grpc.aio.insecure_channel(
                COORDINATOR_ADDRESS, options=GRPC_OPTIONS
            ) as channel:
                stub = compute_pb2_grpc.CoordinatorStub(channel)

                import os

                await stub.RegisterNode(
                    compute_pb2.NodeRegistration(
                        node_id=self.node.node_id,
                        ip="127.0.0.1",
                        port=self.node.port,
                        cpu_cores=os.cpu_count() or 4,
                        memory_gb=8,
                    )
                )
        except Exception:
            pass

    async def update_status(self, status):
        """Update status with coordinator"""
        try:
            async with grpc.aio.insecure_channel(
                COORDINATOR_ADDRESS, options=GRPC_OPTIONS
            ) as channel:
                stub = compute_pb2_grpc.CoordinatorStub(channel)
                await stub.UpdateNodeStatus(
                    compute_pb2.NodeStatus(node_id=self.node.node_id, status=status)
                )
        except:
            pass

    async def get_available_peers(self, num_nodes):
        """Get available peers from coordinator and check P2P"""
        candidates = await self._get_candidates(num_nodes)

        if not candidates:
            return []

        # P2P availability checks in parallel
        availability_tasks = []
        for node in candidates:
            if node["node_id"] != self.node.node_id:
                availability_tasks.append(
                    self._check_availability(node["address"], node["node_id"])
                )
            else:
                availability_tasks.append(
                    asyncio.create_task(asyncio.sleep(0, result=False))
                )

        availability_results = await asyncio.gather(*availability_tasks)

        actually_available = [
            node
            for node, available in zip(candidates, availability_results)
            if available and node["node_id"] != self.node.node_id
        ]

        return actually_available

    async def _get_candidates(self, num_nodes):
        """Get candidate nodes from coordinator"""
        try:
            async with grpc.aio.insecure_channel(
                COORDINATOR_ADDRESS, options=GRPC_OPTIONS
            ) as channel:
                stub = compute_pb2_grpc.CoordinatorStub(channel)

                response = await stub.GetAvailableNodes(
                    compute_pb2.NodeRequest(
                        num_nodes=num_nodes, requesting_node_id=self.node.node_id
                    )
                )

                nodes = []
                for n in response.nodes:
                    nodes.append(
                        {
                            "node_id": n.node_id,
                            "address": n.address,
                            "cpu_cores": n.cpu_cores,
                            "memory_gb": n.memory_gb,
                        }
                    )

                return nodes
        except:
            return []

    async def _check_availability(self, peer_address, peer_id):
        """P2P availability check"""
        try:
            async with grpc.aio.insecure_channel(
                peer_address, options=GRPC_OPTIONS
            ) as channel:
                stub = compute_pb2_grpc.NodeStub(channel)

                response = await stub.CheckAvailability(
                    compute_pb2.AvailabilityRequest(requester_id=self.node.node_id)
                )

                return response.available
        except:
            return False

    async def select_best_nodes(self, available_nodes):
        """Measure latencies and select best nodes"""
        if not available_nodes:
            return []

        # Measure latencies in parallel
        latency_tasks = []
        for node in available_nodes:
            if node["node_id"] not in self.node.peer_latencies:
                latency_tasks.append(self._measure_latency(node["address"]))
            else:
                latency_tasks.append(
                    asyncio.create_task(
                        asyncio.sleep(
                            0, result=self.node.peer_latencies[node["node_id"]]
                        )
                    )
                )

        latencies = await asyncio.gather(*latency_tasks)

        # Build quality list
        node_quality = []
        for node, latency in zip(available_nodes, latencies):
            self.node.peer_latencies[node["node_id"]] = latency
            node_quality.append((node, latency))

        # Sort by latency
        node_quality.sort(key=lambda x: x[1])

        # Select good nodes
        good_nodes = [
            node for node, lat in node_quality if lat < GOOD_CONNECTION_LATENCY
        ]
        if not good_nodes:
            good_nodes = [
                node for node, lat in node_quality[: min(3, len(node_quality))]
            ]

        return good_nodes

    async def _measure_latency(self, node_address):
        """Measure latency to a node"""
        try:
            async with grpc.aio.insecure_channel(
                node_address, options=GRPC_OPTIONS
            ) as channel:
                stub = compute_pb2_grpc.NodeStub(channel)

                start = time.time()
                await stub.Ping(compute_pb2.PingRequest(message="latency_test"))
                latency = time.time() - start

                return latency
        except:
            return 999.0

    async def reserve_peer(self, peer_address, peer_id, task_id):
        """Reserve a peer for task"""
        try:
            async with grpc.aio.insecure_channel(
                peer_address, options=GRPC_OPTIONS
            ) as channel:
                stub = compute_pb2_grpc.NodeStub(channel)

                response = await stub.ReserveForTask(
                    compute_pb2.ReservationRequest(
                        requester_id=self.node.node_id, task_id=task_id
                    )
                )

                return response.reserved
        except:
            return False

    async def send_task(self, node_address, task_id, A_block, B, depth):
        """Send task to node"""
        try:
            if self.latency_simulation > 0:
                await asyncio.sleep(self.latency_simulation)

            async with grpc.aio.insecure_channel(
                node_address, options=GRPC_OPTIONS
            ) as channel:
                stub = compute_pb2_grpc.NodeStub(channel)

                A_bytes = pickle.dumps(A_block)
                B_bytes = pickle.dumps(B)

                response = await stub.ComputeTask(
                    compute_pb2.TaskRequest(
                        task_id=task_id,
                        a_block=A_bytes,
                        b_matrix=B_bytes,
                        a_shape=A_block.shape,
                        b_shape=B.shape,
                    )
                )

                if response.status == "success":
                    return pickle.loads(response.result)
                else:
                    raise Exception(response.status)
        except Exception as e:
            raise

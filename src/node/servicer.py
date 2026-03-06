"""
Async gRPC servicer for node
Handles incoming P2P requests
"""

import numpy as np
import pickle
import asyncio
import sys
import os

# Fix imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import *
import compute_pb2
import compute_pb2_grpc


class AsyncNodeServicer(compute_pb2_grpc.NodeServicer):
    """Async node servicer for P2P requests"""

    def __init__(self, node):
        self.node = node

    async def CheckAvailability(self, request, context):
        """P2P availability check"""
        is_available = (
            self.node.status == "available"
            and self.node.current_load == 0
            and self.node.reserved_by is None
        )

        return compute_pb2.AvailabilityResponse(
            available=is_available,
            status=self.node.status,
            current_load=self.node.current_load,
        )

    async def ReserveForTask(self, request, context):
        """P2P reservation"""
        requester = request.requester_id

        if self.node.status == "available" and self.node.reserved_by is None:
            self.node.reserved_by = requester
            self.node.status = "busy"
            self.node.current_load += 1

            return compute_pb2.ReservationResponse(
                reserved=True, message=f"Reserved for {requester}"
            )
        else:
            return compute_pb2.ReservationResponse(
                reserved=False, message=f"Already {self.node.status}"
            )

    async def ComputeTask(self, request, context):
        """WORKER MODE: Compute or redistribute"""
        task_id = request.task_id

        if self.node.latency_simulation > 0:
            await asyncio.sleep(self.node.latency_simulation)

        try:
            A_block = pickle.loads(request.a_block)
            B = pickle.loads(request.b_matrix)

            task_size = A_block.size + B.size

            # Decision: redistribute or compute?
            if (
                ENABLE_RECURSION
                and task_size > REDISTRIBUTION_SIZE_THRESHOLD
                and A_block.shape[0] > MIN_ROWS_FOR_REDISTRIBUTION
            ):
                other_nodes = await self.node.network._get_candidates(3)

                if len(other_nodes) >= 2:
                    result = await self.node.compute_distributed(A_block, B, depth=1)
                else:
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, np.matmul, A_block, B)
            else:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, np.matmul, A_block, B)

            # Release reservation
            self.node.reserved_by = None
            self.node.current_load -= 1
            if self.node.current_load == 0:
                self.node.status = "available"
                await self.node.update_status("available")

            result_bytes = pickle.dumps(result)
            return compute_pb2.TaskResult(
                task_id=task_id, result=result_bytes, status="success"
            )
        except Exception as e:
            self.node.reserved_by = None
            self.node.current_load -= 1
            if self.node.current_load == 0:
                self.node.status = "available"
            return compute_pb2.TaskResult(
                task_id=task_id, result=b"", status=f"error: {str(e)}"
            )

    async def Ping(self, request, context):
        """Ping for latency measurement"""
        if self.node.latency_simulation > 0:
            await asyncio.sleep(self.node.latency_simulation)
        return compute_pb2.PingResponse(message=f"pong from {self.node.node_id}")

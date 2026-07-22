import networkx as nx
import logging
import json
from graph_builder import ArlandaGraphBuilder

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": "baggage-routing-engine",
            "message": record.getMessage()
        })

logger = logging.getLogger("baggage-routing-engine")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(JSONFormatter())
    logger.addHandler(ch)

class BaggageRoutingEngine:
    def __init__(self):
        logger.info("Initializing Dynamic Baggage Routing Engine...")
        self.builder = ArlandaGraphBuilder()
        
        # Boot up the dynamic map in memory
        self.builder.fetch_real_gates_aip()
        self.builder.inject_synthetic_bhs_infrastructure()
        self.builder.establish_routing_edges()
        
        # Take ownership of the fully wired graph
        self.graph = self.builder.graph
        logger.info(f"Engine Ready. Managing {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} dynamic routing edges.")

    def calculate_optimal_route(self, bag_id, source_node, target_node):
        """
        Calculates the FASTEST path based on dynamic effective_time, not just distance.
        """
        logger.info(f"Calculating dynamic route for Bag {bag_id}: {source_node} -> {target_node}")
        
        try:
            # Tell Dijkstra to use 'effective_time' instead of physical distance
            optimal_path = nx.dijkstra_path(
                self.graph, 
                source=source_node, 
                target=target_node, 
                weight='effective_time'
            )
            
            # Calculate total time and distance for the chosen path
            path_edges = list(zip(optimal_path, optimal_path[1:]))
            total_time_sec = sum(self.graph[u][v]['effective_time'] for u, v in path_edges)
            total_distance = sum(self.graph[u][v]['distance'] for u, v in path_edges)
            
            # If the time is infinity, it means all physical paths are broken
            if total_time_sec == float('inf'):
                raise nx.NetworkXNoPath
                
            route_result = {
                "bag_id": bag_id,
                "status": "ROUTE_CALCULATED",
                "source": source_node,
                "target": target_node,
                "path": optimal_path,
                "metrics": {
                    "estimated_transit_time_seconds": round(total_time_sec, 2),
                    "total_distance_meters": round(total_distance, 2)
                }
            }
            
            logger.info(f"Route successful for Bag {bag_id}. Time: {route_result['metrics']['estimated_transit_time_seconds']}s, Distance: {route_result['metrics']['total_distance_meters']}m.")
            return route_result
            
        except nx.NetworkXNoPath:
            logger.error(f"ROUTING FAILED: No operational path exists between {source_node} and {target_node}.")
            return {"bag_id": bag_id, "status": "FAILED_NO_PATH"}
            
        except nx.NodeNotFound as e:
            logger.error(f"ROUTING ERROR: Invalid node - {str(e)}")
            return {"bag_id": bag_id, "status": "FAILED_INVALID_NODE"}
        

if __name__ == "__main__":
    engine = BaggageRoutingEngine()
    
    print("\n--- SCENARIO 1: Normal Operations ---")
    route1 = engine.calculate_optimal_route("BAG-100", "CheckIn_T5_South", "Gate_12")
    print(json.dumps(route1, indent=2))
    
    print("\n--- SCENARIO 2: Hardware Failure (Self-Healing Demo) ---")
    # SIMULATE A KAFKA EVENT: The tug route from T5 South to Gate 12 is blocked/broken
    engine.builder.update_edge_condition("BHS_Hub_T5_South", "Gate_12", status="broken")
    
    # Recalculate for the exact same bag. The system should automatically find the backup 
    route2 = engine.calculate_optimal_route("BAG-100", "CheckIn_T5_South", "Gate_12")
    print(json.dumps(route2, indent=2))
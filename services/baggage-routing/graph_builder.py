import networkx as nx
import logging
import json
import re
import math

# Setup standard JSON logger
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": "baggage-routing",
            "message": record.getMessage()
        }
        return json.dumps(log_record)

logger = logging.getLogger("baggage-routing")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(JSONFormatter())
logger.addHandler(ch)

# Core Graph Construction Class
class ArlandaGraphBuilder:
    def __init__(self):
        self.graph = nx.Graph()
        
        # Official AIP Sweden INS Coordinates (AD 2 ESSA 2-8, AIRAC AMDT 3/2025)
        self.aip_stands = {
            # Terminal 2
            "Gate_62": ("593841.07N", "0175535.33E"),
            "Gate_63": ("593839.76N", "0175537.21E"),
            "Gate_64": ("593838.55N", "0175539.33E"),
            "Gate_65": ("593837.61N", "0175541.29E"),
            "Gate_66": ("593836.60N", "0175543.79E"),
            "Gate_67": ("593835.72N", "0175546.48E"),
            "Gate_68": ("593835.08N", "0175548.87E"),
            
            # Terminal 3
            "Gate_52": ("593844.98N", "0175533.06E"),
            "Gate_53": ("593842.53N", "0175526.86E"),
            "Gate_54": ("593845.41N", "0175530.06E"),
            "Gate_55": ("593843.02N", "0175523.72E"),
            "Gate_56": ("593845.75N", "0175526.80E"),
            "Gate_57": ("593840.64N", "0175521.78E"),
            "Gate_58": ("593846.10N", "0175523.54E"),
            "Gate_60A": ("593841.98N", "0175530.54E"),
            
            # Terminal 4
            "Gate_31": ("593848.51N", "0175540.72E"),
            "Gate_33": ("593849.63N", "0175538.80E"),
            "Gate_35": ("593850.42N", "0175536.87E"),
            "Gate_37": ("593850.75N", "0175534.30E"),
            "Gate_39": ("593851.06N", "0175531.31E"),
            "Gate_41": ("593851.54N", "0175528.89E"),
            "Gate_43": ("593852.42N", "0175526.98E"),
            
            # Terminal 5 (Pier A)
            "Gate_1": ("593907.41N", "0175549.77E"),
            "Gate_3": ("593908.91N", "0175548.76E"),
            "Gate_4": ("593909.97N", "0175554.08E"),
            "Gate_5": ("593910.64N", "0175547.54E"),
            "Gate_6": ("593911.70N", "0175552.85E"),
            "Gate_7": ("593911.87N", "0175545.57E"),
            "Gate_8": ("593913.28N", "0175552.69E"),
            "Gate_9": ("593913.02N", "0175547.17E"),
            "Gate_10": ("593913.61N", "0175549.50E"),
            
            # Terminal 5 (Pier B)
            "Gate_12": ("593904.44N", "0175544.10E"),
            "Gate_14": ("593904.56N", "0175541.22E"),
            "Gate_16": ("593904.89N", "0175537.62E"),
            "Gate_18": ("593905.61N", "0175534.86E"),
            "Gate_20": ("593904.38N", "0175532.70E"),
            
            # Terminal 5 (Pier F)
            "Gate_F28L": ("593911.07N", "0175617.53E"),
            "Gate_F28R": ("593911.31N", "0175616.14E"),
            "Gate_F29L": ("593910.47N", "0175609.85E"),
            "Gate_F30": ("593912.39N", "0175616.89E"),
            "Gate_F31": ("593911.49N", "0175609.12E"),
            "Gate_F32L": ("593913.98N", "0175616.01E"),
            "Gate_F32R": ("593914.08N", "0175614.76E"),
            "Gate_F33": ("593912.79N", "0175608.28E"),
            "Gate_F34": ("593915.31N", "0175615.50E"),
            "Gate_F35": ("593914.42N", "0175607.75E"),
            "Gate_F36L": ("593916.58N", "0175614.51E"),
            "Gate_F36R": ("593916.77N", "0175613.34E"),
            "Gate_F37": ("593915.72N", "0175607.01E"),
            "Gate_F38": ("593918.13N", "0175613.92E"),
            "Gate_F39L": ("593917.19N", "0175606.35E"),
            "Gate_F39R": ("593916.95N", "0175607.52E"),
            "Gate_F40": ("593917.08N", "0175626.41E"),
            "Gate_F42": ("593918.39N", "0175625.76E"),
            "Gate_F44": ("593919.70N", "0175625.11E"),
            
            # Apron E (Remote Stands)
            "Gate_101": ("593834.85N", "0175600.48E"),
            "Gate_103": ("593833.47N", "0175558.59E"),
            "Gate_105": ("593832.97N", "0175559.33E"),
            "Gate_107": ("593832.10N", "0175600.49E"),
            "Gate_109": ("593830.73N", "0175558.59E"),
            "Gate_111": ("593830.23N", "0175559.33E"),
            "Gate_113": ("593829.35N", "0175600.49E"),
            "Gate_115": ("593827.98N", "0175558.60E"),
            "Gate_117": ("593827.48N", "0175559.34E"),
            "Gate_119": ("593826.61N", "0175600.50E"),

            # Apron D
            "Gate_102": ("593833.02N", "0175551.69E"),
            "Gate_104": ("593832.21N", "0175552.98E"),
            "Gate_106": ("593831.64N", "0175553.59E"),
            "Gate_108": ("593830.09N", "0175552.94E"),
            "Gate_110": ("593829.01N", "0175552.85E"),
            "Gate_112": ("593828.11N", "0175553.55E"),
            
            # Apron R
            "Gate_R3": ("593827.31N", "0175532.65E"),
            "Gate_R4": ("593826.39N", "0175536.09E"),
            "Gate_R5": ("593822.23N", "0175536.13E"),
            "Gate_R6": ("593821.13N", "0175540.20E"),
            "Gate_R7": ("593820.06N", "0175544.32E"),
            "Gate_R8": ("593819.10N", "0175548.58E"),
            "Gate_R9": ("593817.97N", "0175552.60E"),
            "Gate_R9C": ("593819.15N", "0175552.91E"),
            "Gate_R10": ("593816.58N", "0175556.81E"),
            
            # Apron M
            "Gate_M5": ("593846.70N", "0175707.40E"),
            "Gate_M6": ("593846.40N", "0175705.93E"),
            "Gate_M7": ("593845.85N", "0175704.75E"),
            "Gate_M8": ("593844.65N", "0175701.39E"),
            "Gate_M9": ("593843.61N", "0175658.15E"),

            # Apron K
            "Gate_K1": ("593929.62N", "0175729.35E"),
            "Gate_K2": ("593927.62N", "0175730.73E"),
            "Gate_K3A": ("593928.02N", "0175734.47E"),
            "Gate_K3B": ("593928.69N", "0175736.90E"),
            "Gate_K3C": ("593929.02N", "0175739.51E"),
            "Gate_K3D": ("593928.00N", "0175734.19E"),
            "Gate_K3E": ("593928.86N", "0175738.04E"),
            "Gate_K5": ("593930.57N", "0175751.82E"),
            "Gate_K5L": ("593930.70N", "0175752.84E"),
            "Gate_K5R": ("593930.37N", "0175750.34E"),

            # Apron G
            "Gate_G141": ("593909.33N", "0175637.02E"),
            "Gate_G142": ("593911.28N", "0175635.82E"),
            "Gate_G143": ("593913.12N", "0175634.87E"),
            "Gate_G144": ("593914.97N", "0175633.93E"),
            "Gate_G145": ("593916.84N", "0175633.00E"),
            "Gate_G146": ("593918.68N", "0175632.09E"),
            "Gate_G148": ("593920.53N", "0175631.16E"),
            "Gate_G149": ("593920.05N", "0175633.16E"),
            
            # Apron S
            "Gate_S1": ("593830.69N", "0175518.15E"),
            "Gate_S2": ("593828.82N", "0175517.47E"),
            "Gate_S3": ("593826.93N", "0175516.79E"),
            "Gate_S4": ("593825.48N", "0175515.12E"),
            "Gate_S5": ("593830.52N", "0175514.12E"),
            "Gate_S6": ("593823.63N", "0175513.48E"),
            "Gate_S71": ("593826.25N", "0175509.84E"),
            "Gate_S72": ("593825.08N", "0175509.40E"),  
            "Gate_S73": ("593823.90N", "0175508.99E"),  
            "Gate_S74": ("593822.72N", "0175508.56E"),  
            "Gate_S75": ("593821.54N", "0175508.14E"),  
            "Gate_S77": ("593819.20N", "0175507.27E"), 
            "Gate_S78": ("593818.02N", "0175506.86E"),  
            "Gate_S79": ("593816.84N", "0175506.43E"),  
            "Gate_S80": ("593817.28N", "0175510.56E"),  
            "Gate_S81": ("593816.04N", "0175510.15E"),  
            "Gate_S82": ("593815.19N", "0175506.61E"),
            
            # Apron ACLs (Aircraft Clearance Lines)
            "Gate_Apron_HACL": ("593919.94N", "0175652.30E"),
            "Gate_Apron_JACL": ("593927.08N", "0175715.73E"),
            "Gate_Apron_LACL": ("593935.69N", "0175826.21E")
        }
        
    def inject_synthetic_bhs_infrastructure(self):
        """
        Injects mock Check-in and Baggage Handling System (BHS) hubs 
        based on the geographic layout of Arlanda's terminals.
        """
        logger.info("Injecting synthetic BHS hubs and Check-in nodes...")
        
        # Geographically approximated using the LFV AIP chart footprint
        self.synthetic_infrastructure = {
            # Terminal 2 (2 Carousels)
            "CheckIn_T2": {"lat": 59.6430, "lon": 17.9270, "type": "check_in"},
            "BHS_Hub_T2": {"lat": 59.6435, "lon": 17.9275, "type": "bhs_hub"},
            "Carousel_T2_1": {"lat": 59.6431, "lon": 17.9271, "type": "carousel"},
            "Carousel_T2_2": {"lat": 59.6432, "lon": 17.9272, "type": "carousel"},
            
            # Terminal 3 (Regional - 1 Carousel)
            "CheckIn_T3": {"lat": 59.6450, "lon": 17.9240, "type": "check_in"},
            "BHS_Hub_T3": {"lat": 59.6455, "lon": 17.9245, "type": "bhs_hub"},
            "Carousel_T3_1": {"lat": 59.6451, "lon": 17.9241, "type": "carousel"},
            
            # Terminal 4 (Domestic - 3 Carousels)
            "CheckIn_T4": {"lat": 59.6470, "lon": 17.9220, "type": "check_in"},
            "BHS_Hub_T4": {"lat": 59.6475, "lon": 17.9225, "type": "bhs_hub"},
            "Carousel_T4_1": {"lat": 59.6471, "lon": 17.9221, "type": "carousel"},
            "Carousel_T4_2": {"lat": 59.6472, "lon": 17.9222, "type": "carousel"},
            "Carousel_T4_3": {"lat": 59.6473, "lon": 17.9223, "type": "carousel"},
            
            # Terminal 5 (Massive International - Split into North/South)
            # South (Pier A/B area - 3 Carousels)
            "CheckIn_T5_South": {"lat": 59.6505, "lon": 17.9310, "type": "check_in"}, 
            "BHS_Hub_T5_South": {"lat": 59.6510, "lon": 17.9315, "type": "bhs_hub"},
            "Carousel_T5_1": {"lat": 59.6506, "lon": 17.9311, "type": "carousel"},
            "Carousel_T5_2": {"lat": 59.6507, "lon": 17.9312, "type": "carousel"},
            "Carousel_T5_3": {"lat": 59.6508, "lon": 17.9313, "type": "carousel"},
            
            # North (Pier F area - 3 Carousels)
            "CheckIn_T5_North": {"lat": 59.6535, "lon": 17.9340, "type": "check_in"}, 
            "BHS_Hub_T5_North": {"lat": 59.6540, "lon": 17.9345, "type": "bhs_hub"},
            "Carousel_T5_4": {"lat": 59.6536, "lon": 17.9341, "type": "carousel"},
            "Carousel_T5_5": {"lat": 59.6537, "lon": 17.9342, "type": "carousel"},
            "Carousel_T5_6": {"lat": 59.6538, "lon": 17.9343, "type": "carousel"},
        }
        
        count = 0
        for node_id, data in self.synthetic_infrastructure.items():
            self.graph.add_node(
                node_id, type=data["type"], lat=data["lat"], lon=data["lon"]
            )
            count += 1
            
        logger.info(f"Successfully injected {count} synthetic infrastructure nodes.")

    def dms_to_decimal(self, dms_str):
        """Converts aviation Degrees-Minutes-Seconds to Decimal Degrees."""
        match = re.match(r"(\d{2,3})(\d{2})(\d{2}\.\d{2})([NE])", dms_str)
        if not match:
            logger.error(f"Failed to parse coordinate string: {dms_str}")
            return 0.0
            
        degrees, minutes, seconds, direction = match.groups()
        
        # Core mathematical conversion formula
        decimal = float(degrees) + (float(minutes) / 60) + (float(seconds) / 3600)
        return round(decimal, 7)

    def fetch_real_gates_aip(self):
        logger.info("Loading official AIP Sweden INS stand coordinates into the graph...")
        count = 0
        
        for gate_id, (lat_dms, lon_dms) in self.aip_stands.items():
            # Convert raw strings to mathematical decimal coordinates
            lat_dec = self.dms_to_decimal(lat_dms)
            lon_dec = self.dms_to_decimal(lon_dms)
            
            self.graph.add_node(
                gate_id, type="gate", lat=lat_dec, lon=lon_dec
            )
            count += 1
            
        logger.info(f"Successfully loaded {count} physical gates from AIP document.")

    def project_real_nodes(self):
        """Projects all authentic nodes currently loaded into the graph."""
        logger.info("Projecting all real nodes currently in the graph:")
        nodes = dict(self.graph.nodes(data=True))
        
        # Sort them alphanumerically for easier reading in the terminal
        for node_id in sorted(nodes.keys()):
            attrs = nodes[node_id]
            print(f"- {node_id}: (Lat: {attrs['lat']}, Lon: {attrs['lon']})")
            
        logger.info(f"Total authentic gate nodes mapped: {self.graph.number_of_nodes()}")
        
    def calculate_haversine_distance(self, lat1, lon1, lat2, lon2):
        """
        Calculates the great-circle distance between two points on the Earth in meters.
        """
        R = 6371000  
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
        
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        distance_meters = R * c
        return round(distance_meters, 2)

    def establish_routing_edges(self):
        """
        Precalculates edges using dynamic TIME (seconds) instead of static distance.
        Binds gates to the TWO nearest hubs to allow for algorithmic rerouting.
        """
        logger.info("Precalculating dynamic routing edges based on time and speed...")
        nodes = dict(self.graph.nodes(data=True))
        edge_count = 0

        # Speeds in meters per second
        CONVEYOR_SPEED = 2.0  # Standard belt
        TUG_SPEED = 5.0       # Ground vehicle
        TRANSFER_SPEED = 10.0 # High-speed underground rail

        # Check-in to BHS Hubs (Conveyor)
        check_ins = [n for n, d in nodes.items() if d.get("type") == "check_in"]
        for ci in check_ins:
            hub = ci.replace("CheckIn", "BHS_Hub")
            if hub in nodes:
                dist = 50.0
                base_time = dist / CONVEYOR_SPEED
                self.graph.add_edge(ci, hub, distance=dist, base_time=base_time, effective_time=base_time, edge_type="conveyor")
                edge_count += 1

        # BHS Hubs to Carousels (Conveyor)
        carousels = [n for n, d in nodes.items() if d.get("type") == "carousel"]
        for car in carousels:
            hub = None
            if "T2" in car: hub = "BHS_Hub_T2"
            elif "T3" in car: hub = "BHS_Hub_T3"
            elif "T4" in car: hub = "BHS_Hub_T4"
            elif "T5" in car and int(car.split("_")[-1]) <= 3: hub = "BHS_Hub_T5_South"
            elif "T5" in car and int(car.split("_")[-1]) > 3:  hub = "BHS_Hub_T5_North"

            if hub and hub in nodes:
                dist = 30.0
                base_time = dist / CONVEYOR_SPEED
                self.graph.add_edge(hub, car, distance=dist, base_time=base_time, effective_time=base_time, edge_type="conveyor")
                edge_count += 1

        # BHS Hubs to Gates (Tug Routes) -> NOW CONNECTS TO TWO CLOSEST HUBS
        gates = [n for n, d in nodes.items() if d.get("type") == "gate"]
        bhs_hubs = [n for n, d in nodes.items() if d.get("type") == "bhs_hub"]

        for gate in gates:
            gate_data = nodes[gate]
            hub_distances = []
            
            for hub in bhs_hubs:
                hub_data = nodes[hub]
                dist = self.calculate_haversine_distance(
                    gate_data["lat"], gate_data["lon"], hub_data["lat"], hub_data["lon"]
                )
                hub_distances.append((hub, dist))
            
            # Sort by distance and take the TOP 2 closest hubs
            hub_distances.sort(key=lambda x: x[1])
            closest_hubs = hub_distances[:2]
            
            for hub, dist in closest_hubs:
                base_time = dist / TUG_SPEED
                self.graph.add_edge(hub, gate, distance=dist, base_time=base_time, effective_time=base_time, edge_type="tug_route")
                edge_count += 1

        # Hub to Hub 
        for i in range(len(bhs_hubs)):
            for j in range(i + 1, len(bhs_hubs)):
                hub_a = bhs_hubs[i]
                hub_b = bhs_hubs[j]
                dist = self.calculate_haversine_distance(
                    nodes[hub_a]["lat"], nodes[hub_a]["lon"], nodes[hub_b]["lat"], nodes[hub_b]["lon"]
                )
                base_time = dist / TRANSFER_SPEED
                self.graph.add_edge(hub_a, hub_b, distance=dist, base_time=base_time, effective_time=base_time, edge_type="transfer_track")
                edge_count += 1

        logger.info(f"Successfully configured {edge_count} dynamic routing edges.")

    def update_edge_condition(self, node_a, node_b, status="operational", congestion_multiplier=1.0):
        """Dynamically updates the effective travel time between two nodes."""
        if self.graph.has_edge(node_a, node_b):
            if status == "broken":
                # Infinite time means the algorithm will completely avoid this path
                self.graph[node_a][node_b]['effective_time'] = float('inf')
                logger.warning(f"ALERT: Edge {node_a} <-> {node_b} marked as BROKEN.")
            else:
                base = self.graph[node_a][node_b]['base_time']
                self.graph[node_a][node_b]['effective_time'] = base * congestion_multiplier
                logger.info(f"UPDATE: Edge {node_a} <-> {node_b} congestion set to {congestion_multiplier}x.")


if __name__ == "__main__":
    builder = ArlandaGraphBuilder()
    builder.fetch_real_gates_aip()
    builder.inject_synthetic_bhs_infrastructure()
    builder.project_real_nodes()
    print(f"Total Nodes: {builder.graph.number_of_nodes()}")
    print(f"Total Edges: {builder.graph.number_of_edges()}")
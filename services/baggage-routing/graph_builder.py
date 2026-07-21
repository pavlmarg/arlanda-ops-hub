import networkx as nx
import logging
import json
import re

# Setup standard JSON logger
class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "service": "baggage-routing-service",
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
        
        # Official AIP Sweden INS Coordinates (AD 2 ESSA 2-8)
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
            "Gate_3": ("593908.91N", "0175548.76E"),
            "Gate_4": ("593909.97N", "0175554.08E"),
            "Gate_5": ("593910.64N", "0175547.54E"),
            "Gate_6": ("593911.70N", "0175552.85E"),
            "Gate_7": ("593911.87N", "0175545.57E"),
            "Gate_8": ("593913.28N", "0175552.69E"),
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
            
            # Apron R
            "Gate_R3": ("593827.31N", "0175532.65E"),
            "Gate_R4": ("593826.39N", "0175536.09E"),
            "Gate_R5": ("593822.23N", "0175536.13E"),
            "Gate_R6": ("593821.13N", "0175540.20E"),
            "Gate_R7": ("593820.06N", "0175544.32E"),
            "Gate_R8": ("593819.10N", "0175548.58E"),
            "Gate_R9": ("593817.97N", "0175552.60E"),
            "Gate_R10": ("593816.58N", "0175556.81E"),

            # Apron G
            "Gate_G141": ("593909.33N", "0175637.02E"),
            "Gate_G142": ("593911.28N", "0175635.82E"),
            "Gate_G143": ("593913.12N", "0175634.87E"),
            "Gate_G144": ("593914.97N", "0175633.93E"),
            "Gate_G145": ("593916.84N", "0175633.00E"),
            "Gate_G146": ("593918.68N", "0175632.09E"),
            
            # Apron S
            "Gate_S1": ("593830.69N", "0175518.15E"),
            "Gate_S2": ("593828.82N", "0175517.47E"),
            "Gate_S3": ("593826.93N", "0175516.79E"),
            "Gate_S4": ("593825.48N", "0175515.12E"),
            "Gate_S5": ("593830.52N", "0175514.12E"),
            "Gate_S6": ("593823.63N", "0175513.48E")
        }

    def dms_to_decimal(self, dms_str):
        """Converts aviation Degrees-Minutes-Seconds (e.g., 593841.07N) to Decimal Degrees."""
        # Regex to capture: Degrees (2-3 chars), Minutes (2 chars), Seconds (4-5 chars), Direction (N/E)
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
                gate_id, 
                type="gate", 
                lat=lat_dec, 
                lon=lon_dec
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


if __name__ == "__main__":
    builder = ArlandaGraphBuilder()
    
    # Execute the internal AIP data loading
    builder.fetch_real_gates_aip()
    builder.project_real_nodes()
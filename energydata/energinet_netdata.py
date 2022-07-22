import io
import urllib.request
import openpyxl
import networkx as nx
from . import energinet

netdata_url = "https://energinet.dk/-/media/76C3D434A7E2499F94E7DB66D3A2DE21.xlsx?la=da&hash=638E165AA57F627409CB9394D56D53614F5D7336"

extra_station_codes = {
    'A2': 'BJS',
    'AMF_132_F': 'AMV',
    'HASØ132_F': 'HAS',
    'ISHN132_N-$': 'ISH',
    'MRP1': 'TEG',
    'VSAK132_S1': 'VAL',
    'BIX_150_S1': 'BIL',
    'FRX_150_S1': 'FRT',
    'I4': 'ABS',
    'KLT_150_S1': 'KLF',
    'VKHI012_SFIK': 'VKE',
    'term_KT51-HT3': 'TJE',
}

def fetch_connectionpoints():
    descriptor = {"resource_id": "connectionpointsingrid"}
    service = energinet.EnergiDataServiceDk(descriptor)
    metadata = service.fetch_metadata()
    data = service.fetch_data()
    connection_points = {
        row["ConnectionPointCode"]: row for row in data
    }
    return connection_points

def fetch_netdata_workbook():    
    with urllib.request.urlopen(netdata_url) as fileobj:
        buf = io.BytesIO()
        buf.write(fileobj.read())
    buf.seek(0)
    wb = openpyxl.load_workbook(buf)
    return wb

def get_sheet_data(ws):
    [_1, _2, _3, header, *rows] = ws.values
    for row in rows:
        yield {k: v for k, v in zip(header, row)}
        
import re
def match_bus_to_code(d, codes, extra_codes):
    extensions = {
        "ENDKW": "V",
        "ENDKE": "Ø"
    }
    name = d["Bus Name"]
    if name in extra_codes and extra_codes[name] in codes:
        return extra_codes[name]
    name_regex = re.compile(r"([^0-9_]+)_?(\d*)_?(.*)")
    match = name_regex.match(name)
    if not match:
        return
    bus_code = match.groups()[0]
    if bus_code in codes:
        return bus_code
    star_codes = [f"{bus_code}*", f"{bus_code}**", f"{bus_code}***"]
    for star_code in star_codes:
        if star_code in codes:
            return star_code
    if d["Location Name"] in extensions:
        area_code = bus_code + extensions[d["Location Name"]]
        if area_code in codes:
            return area_code

def generate_label(type, name):
    return f"{type}:{name}"
    
def build_graph(connection_points, wb):
    energy_grid = nx.Graph()
    bus_labels = {}
    
    for d in get_sheet_data(wb["Bus"]):
        node_label = generate_label("Bus", d["Bus Name"])
        bus_labels[d["Bus Index"]] = node_label
        attributes = {k: v for k, v in d.items()}
        connection_code = match_bus_to_code(d, list(connection_points), extra_station_codes)
        if connection_code is not None:
            attributes.update(connection_points[connection_code])
        energy_grid.add_nodes_from([(node_label, attributes)])
        
    for d in get_sheet_data(wb["Line"]):
        energy_grid.add_edges_from([(bus_labels[d["Node 1"]], bus_labels[d["Node 2"]], d)])
        
    for d in get_sheet_data(wb["Transformer2"]):
        node_label = generate_label("Transformer2", d["2-Transformer Name"])    
        energy_grid.add_nodes_from([(node_label, d)])
        energy_grid.add_edges_from([
            (bus_labels[d["High.V Bus Index"]], node_label), 
            (bus_labels[d["Low.V Bus Index"]], node_label)
        ])
     
    for d in get_sheet_data(wb["Transformer3"]):
        node_label = generate_label("Transformer3", d["3_Transformer Name"])
        energy_grid.add_nodes_from([(node_label, d)])
        edges = [
            (bus_labels[d["High.V Bus Index"]], node_label),
            (bus_labels[d["Mid.V Bus Index"]], node_label),        
            (bus_labels[d["Low.V Bus Index"]], node_label)
        ]
        energy_grid.add_edges_from(edges)
        
    for d in get_sheet_data(wb["Generator"]):
        node_label = generate_label("Generator", d["Generator Name"])
        energy_grid.add_nodes_from([(node_label, d)])
        energy_grid.add_edge(node_label, bus_labels[d["Bus Index"]])
        
    for d in get_sheet_data(wb["Load"]):
        node_label = generate_label("Load", d["Load Name"])
        energy_grid.add_nodes_from([(node_label, d)])
        energy_grid.add_edge(node_label, bus_labels[d["Load Index"]])
        
    for d in get_sheet_data(wb["Shunt"]):
        node_label = generate_label("Shunt", d["Shunt Name"])
        energy_grid.add_nodes_from([(node_label, d)])
        energy_grid.add_edge(node_label, bus_labels[d["Bus Index"]])
        
    for d in get_sheet_data(wb["HVDC"]):
        node_label = generate_label("HVDC", d["HVDC Name"])
        energy_grid.add_nodes_from([(node_label, d)])
        energy_grid.add_edge(node_label, bus_labels[d["Bus Index"]])
        
    return energy_grid

class EnerginetNetdata:
    def fetch_metadata(self):
        return {"type": "graph"}

    def fetch(self):
        connection_points = fetch_connectionpoints()
        wb = fetch_netdata_workbook()
        energy_grid = build_graph(connection_points, wb)
        return nx.readwrite.json_graph.node_link_data(energy_grid)
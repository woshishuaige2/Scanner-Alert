import xml.etree.ElementTree as ET

def parse_float_data(xml_string):
    """Parse float and other key metrics from IBKR ReportSnapshot XML"""
    try:
        root = ET.fromstring(xml_string)
        
        # IBKR XML structure for ReportSnapshot
        # Float is usually under <Ratios Type="Financial"> or <Ratios Type="ShareStats">
        # Look for <Ratio FieldName="FLOAT"> or similar
        
        metrics = {
            'float': None,
            'market_cap': None,
            'avg_volume_90d': None
        }
        
        # Find Float
        for ratio in root.findall(".//Ratio"):
            field_name = ratio.get('FieldName')
            if field_name == 'FLOAT':
                metrics['float'] = float(ratio.text)
            elif field_name == 'MKTCAP':
                metrics['market_cap'] = float(ratio.text)
            elif field_name == 'VOL10DAVG':
                metrics['avg_volume_10d'] = float(ratio.text)
                
        return metrics
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return None

# Example usage with dummy XML
dummy_xml = """
<ReportSnapshot>
    <Ratios Type="ShareStats">
        <Ratio FieldName="FLOAT">15000000</Ratio>
        <Ratio FieldName="MKTCAP">500000000</Ratio>
        <Ratio FieldName="VOL10DAVG">1000000</Ratio>
    </Ratios>
</ReportSnapshot>
"""
if __name__ == "__main__":
    print(parse_float_data(dummy_xml))

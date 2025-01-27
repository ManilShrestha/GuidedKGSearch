from SPARQLWrapper import SPARQLWrapper, JSON
import json
from collections import defaultdict
import time
import requests
from typing import Dict, List, Set, Tuple

class EnhancedMedicalKGExtractor:
    def __init__(self):
        self.endpoint_url = "https://query.wikidata.org/sparql"
        self.sparql = SPARQLWrapper(self.endpoint_url)
        self.sparql.setReturnFormat(JSON)
        self.sparql.agent = 'MedicalKGExtractor/1.0 (research project)'
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'MedicalKGExtractor/1.0 (research project)'
        })
        
        # Cache for entity labels and descriptions
        self.entity_metadata = {}
        
        # Same medical properties as before
        self.medical_properties = {
            'P31': 'instance of',
            'P279': 'subclass of',
            'P780': 'symptoms',
            'P828': 'has cause',
            'P927': 'anatomical location',
            'P2176': 'drug used for treatment',
            'P1050': 'medical condition',
            'P1995': 'health specialty',
            'P2293': 'genetic association',
            'P1542': 'has effect',
            'P1060': 'pathogen transmission process',
            'P780': 'symptoms',
            'P2849': 'produced by',
            'P2176': 'drug used for treatment',
            'P2175': 'medical condition treated',
            'P3489': 'pregnancy category',
            'P3433': 'biological pathway',
            'P3781': 'has active ingredient'
        }

        self.seed_conditions = {
            'Q199804': 'Asthma',
            'Q199766': 'Chronic obstructive pulmonary disease',
            'Q623067': 'Emphysema',
            'Q1496829': 'Chronic bronchitis',
            'Q1397391': 'Bronchiectasis'
        }

    def get_entity_metadata(self, entity_ids: List[str]) -> Dict[str, Dict]:
        """
        Fetch labels and descriptions for multiple entities in batch
        """
        if not entity_ids:
            return {}
            
        # Convert list to space-separated string of entity IDs
        entities_str = ' '.join(f'wd:{eid}' for eid in entity_ids)
        
        query = f"""
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?entity ?label ?description WHERE {{
          VALUES ?entity {{ {entities_str} }}
          ?entity rdfs:label ?label .
          OPTIONAL {{ ?entity schema:description ?description . }}
          FILTER(LANG(?label) = "en")
          FILTER(LANG(?description) = "en" || !BOUND(?description))
        }}
        """
        
        try:
            self.sparql.setQuery(query)
            results = self.sparql.query().convert()
            
            metadata = {}
            for result in results["results"]["bindings"]:
                entity_id = result["entity"]["value"].split('/')[-1]
                metadata[entity_id] = {
                    'label': result.get("label", {}).get("value", ""),
                    'description': result.get("description", {}).get("value", "")
                }
            return metadata
            
        except Exception as e:
            print(f"Error fetching entity metadata: {str(e)}")
            return {}

    def extract_triples_with_metadata(self, entity_data: Dict) -> List[Dict]:
        """
        Extract triples with enhanced metadata
        """
        enhanced_triples = []
        if not entity_data or 'claims' not in entity_data:
            return enhanced_triples

        entity_id = entity_data['id']
        claims = entity_data['claims']
        
        for prop_id, prop_claims in claims.items():
            if prop_id not in self.medical_properties:
                continue
                
            for claim in prop_claims:
                mainsnak = claim.get('mainsnak', {})
                if mainsnak.get('datatype') == 'wikibase-item':
                    value = mainsnak.get('datavalue', {}).get('value', {})
                    if value and 'id' in value:
                        target_id = value['id']
                        
                        # Create enhanced triple with metadata
                        triple = {
                            'source': {
                                'id': entity_id,
                                'metadata': self.entity_metadata.get(entity_id, {})
                            },
                            'predicate': {
                                'id': prop_id,
                                'label': self.medical_properties[prop_id]
                            },
                            'target': {
                                'id': target_id,
                                'metadata': self.entity_metadata.get(target_id, {})
                            },
                            'qualifiers': []  # Add qualifiers if available in claim
                        }
                        
                        # Add qualifiers if present
                        if 'qualifiers' in claim:
                            for qual_id, qual_values in claim['qualifiers'].items():
                                for qual in qual_values:
                                    if qual.get('datavalue'):
                                        triple['qualifiers'].append({
                                            'property': qual_id,
                                            'value': qual['datavalue'].get('value')
                                        })
                        
                        enhanced_triples.append(triple)
                        
        return enhanced_triples

    def expand_subgraph(self, seed_entities: List[str], max_depth: int = 4) -> Dict:
        """
        Expand the subgraph with enhanced metadata
        """
        entities = {}
        all_triples = []
        entities_to_process = [(seed, 0) for seed in seed_entities]
        processed_entities = set()
        
        total_entities = len(seed_entities)
        current_entity = 0

        while entities_to_process:
            batch_size = min(50, len(entities_to_process))  # Process in batches
            current_batch = entities_to_process[:batch_size]
            entities_to_process = entities_to_process[batch_size:]
            
            # Fetch metadata for the batch
            batch_ids = [eid for eid, _ in current_batch]
            new_metadata = self.get_entity_metadata(batch_ids)
            self.entity_metadata.update(new_metadata)
            
            for current_id, depth in current_batch:
                current_entity += 1
                
                if current_id in processed_entities or depth >= max_depth:
                    continue
                    
                processed_entities.add(current_id)
                
                print(f"Processing entity {current_entity}/{total_entities} at depth {depth}")
                
                time.sleep(0.1)  # Rate limiting
                
                entity_data = self.get_entity_data(current_id)
                if entity_data:
                    entities[current_id] = {
                        'data': entity_data,
                        'metadata': self.entity_metadata.get(current_id, {})
                    }
                    
                    triples = self.extract_triples_with_metadata(entity_data)
                    all_triples.extend(triples)
                    
                    # Add new entities to process
                    for triple in triples:
                        target_id = triple['target']['id']
                        if target_id not in processed_entities:
                            entities_to_process.append((target_id, depth + 1))
                            total_entities += 1

        return {
            'entities': entities,
            'triples': all_triples
        }

    def analyze_subgraph(self, subgraph: Dict) -> Dict:
        """
        Enhanced analysis of the knowledge graph
        """
        analysis = {
            'entity_count': len(subgraph['entities']),
            'triple_count': len(subgraph['triples']),
            'property_distribution': defaultdict(int),
            'entity_types': defaultdict(list),
            'disease_connections': defaultdict(list),
            'hub_entities': defaultdict(dict)
        }
        
        for triple in subgraph['triples']:
            prop_id = triple['predicate']['id']
            source_id = triple['source']['id']
            target_id = triple['target']['id']
            
            # Count property usage
            analysis['property_distribution'][prop_id] += 1
            
            # Track entity connections with metadata
            source_label = triple['source']['metadata'].get('label', source_id)
            target_label = triple['target']['metadata'].get('label', target_id)
            
            if prop_id == 'P31':  # instance of
                analysis['entity_types'][target_label].append(source_label)
            
            # Track disease connections
            connection = {
                'target': target_label,
                'relation': self.medical_properties[prop_id],
                'qualifiers': triple['qualifiers']
            }
            analysis['disease_connections'][source_label].append(connection)
            
            # Track hub entities with metadata
            for entity_id, metadata in [(source_id, triple['source']['metadata']), 
                                      (target_id, triple['target']['metadata'])]:
                if entity_id not in analysis['hub_entities']:
                    analysis['hub_entities'][entity_id] = {
                        'label': metadata.get('label', entity_id),
                        'description': metadata.get('description', ''),
                        'connection_count': 0
                    }
                analysis['hub_entities'][entity_id]['connection_count'] += 1
        
        # Convert to regular dicts and sort
        analysis['property_distribution'] = dict(analysis['property_distribution'])
        analysis['entity_types'] = dict(analysis['entity_types'])
        analysis['disease_connections'] = dict(analysis['disease_connections'])
        
        # Sort hub entities by connection count
        analysis['hub_entities'] = dict(sorted(
            analysis['hub_entities'].items(),
            key=lambda x: x[1]['connection_count'],
            reverse=True
        )[:20])
        
        return analysis

    def get_entity_data(self, entity_id: str) -> Dict:
        """
        Fetch entity data from Wikidata API (same as before)
        """
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                url = f"https://www.wikidata.org/wiki/Special:EntityData/{entity_id}.json"
                response = self.session.get(url)
                response.raise_for_status()
                data = response.json()
                return data['entities'][entity_id]
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    print(f"Retry {attempt + 1} for entity {entity_id}: {str(e)}")
                    time.sleep(retry_delay * (attempt + 1))
                else:
                    print(f"Failed to fetch entity {entity_id} after {max_retries} attempts: {str(e)}")
                    return None
            except Exception as e:
                print(f"Error processing entity {entity_id}: {str(e)}")
                return None

    def create_medical_subgraph(self, max_depth: int = 4, output_file: str = 'enhanced_medical_kg.json'):
        """
        Create and save an enhanced medical knowledge graph
        """
        print("Starting with seed conditions:", self.seed_conditions)
        
        # Initialize entity metadata with seed conditions
        self.entity_metadata.update({
            id_: {'label': label, 'description': ''} 
            for id_, label in self.seed_conditions.items()
        })
        
        # Get additional related conditions with metadata
        related_conditions = self.get_related_conditions(self.seed_conditions)
        all_seeds = list(self.seed_conditions.keys()) + related_conditions
        
        print(f"Expanding subgraph from {len(all_seeds)} seed conditions...")
        subgraph = self.expand_subgraph(all_seeds, max_depth)
        
        # Analyze the enhanced subgraph
        analysis = self.analyze_subgraph(subgraph)
        
        output_data = {
            'metadata': {
                'seed_conditions': self.seed_conditions,
                'max_depth': max_depth,
                'analysis': analysis
            },
            'subgraph': subgraph
        }
        
        print(f"Saving enhanced subgraph...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        return output_data

def main():
    extractor = EnhancedMedicalKGExtractor()
    result = extractor.create_medical_subgraph(
        max_depth=4,
        output_file='enhanced_copd_kg_4hops.json'
    )
    
    # Print enhanced analysis
    analysis = result['metadata']['analysis']
    print("\nEnhanced Knowledge Graph Analysis:")
    print(f"Total entities: {analysis['entity_count']}")
    print(f"Total triples: {analysis['triple_count']}")
    
    print("\nTop property types:")
    for prop_id, count in analysis['property_distribution'].items():
        prop_name = extractor.medical_properties.get(prop_id, prop_id)
        print(f"{prop_name}: {count}")
    
    print("\nTop hub entities:")
    for entity_id, info in analysis['hub_entities'].items():
        print(f"{info['label']} ({entity_id}): {info['connection_count']} connections")
        if info['description']:
            print(f"  Description: {info['description']}")

if __name__ == "__main__":
    main()
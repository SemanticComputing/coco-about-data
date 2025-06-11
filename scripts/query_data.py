
from datetime import datetime
import re
import polars as pl

import rdflib as rdflib
from rdflib.namespace import XSD, RDF, RDFS, Namespace, SKOS, OWL
from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3, RDFXML

import warnings
warnings.filterwarnings("ignore", category=SyntaxWarning) 



############ GET CREDENTIALS   ############
ENDPOINT = "http://ldf.fi/coco/sparql"

def get_endpoint():

  sparql = SPARQLWrapper(ENDPOINT)
  sparql.setReturnFormat(JSON)
  sparql.setMethod(POST)
  #sparql.setCredentials(user, password)
  return sparql


############ COCO PREFIXES, ENDPOINT AND CONSTANTS  ############



PREFIXES = """PREFIX bioc: <http://ldf.fi/schema/bioc/>
PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX schema: <http://schema.org/>
PREFiX skosxl: <http://www.w3.org/2008/05/skos-xl#>
PREFIX text: <http://jena.apache.org/text#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

PREFIX : <http://ldf.fi/schema/coco/>
PREFIX actors: <http://ldf.fi/coco/actors/>
PREFIX events: <http://ldf.fi/coco/events/>
PREFIX letters: <http://ldf.fi/coco/letters/>
PREFIX places: <http://ldf.fi/coco/places/>
PREFIX portal: <http://ldf.fi/coco/portal/>
"""


DATASOURCE_SHORT_LABELS = {'Åbo Akademi University Library':'Åbo Akademi',
 'The National Library of Finland':'National Library',
 "The National Archives of Finland":'National Archives',
 'Finnish Literature Society':'SKS',
 'The Society of Swedish Literature in Finland':'SLS',
 'J. V. Snellman Letters':'Snellman Letters',
 'Elias Lönnrot Letters':'Lönnrot Letters',
 'Albert Edelfelt Letters':'Edelfelt Letters',
 'Finnish National Gallery':'National Gallery',
 'Finnish National Gallery (Word files)':'National Gallery',
 'Serlachius Museums':'Serlachius Museums',
 'Zacharias Topelius Writings':'Topelius Letters',
 'Gallen-Kallela Museum':'Gallen-Kallela Museum',
 'Migration Institute of Finland':'Migration Institute',
 'Migration Institute of Finland (Word files)':'Migration Institute',
 'Postal Museum':'Postal Museum',
 'Aalto University Archives': 'Aalto University',
 'Theatre Museum':'Theatre Museum',
 'The Archives of President Urho Kekkonen':'Kekkonen Archives'
}

############ FUNCTIONS FOR HANDLING QUERY RESULTS ############

def checkDate(v):
  try:
    d = datetime.strptime(v, '%Y-%m-%dT%H:%M:%S').date()
  except ValueError:
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', v)
    d = datetime(int(m.groups()[0]), int(m.groups()[1]), 28).date()
  return d

def createList(occupations):
    occ = occupations.split(";")
    return occ

DATATYPECONVERTERS = {
      str(XSD.integer):  int,
      str(XSD.decimal):  float,
      str(XSD.date):     lambda v: datetime.strptime(v, '%Y-%m-%d').date(),
      str(XSD.dateTime): checkDate,
      str(XSD.string): createList
  }

def convertDatatype(obj):
  if obj.get('datatype'):
    return DATATYPECONVERTERS.get(obj.get('datatype'), str)(obj.get('value'))
  else:
    return str(obj.get('value')).replace('\u0001', '?')

def convertDatatypes(results):
    res = results["results"]["bindings"]
    return [dict([(k, convertDatatype(v)) for k,v in r.items()]) for r in res]


def JSON2Polars(results):
    return pl.from_dicts(convertDatatypes(results))


############ QUERY FUNCTIONS ############

def query_datasources(sparql, return_table=True, save=None):
   source_query = """
    SELECT DISTINCT ?datasource ?label
    WHERE {
    [] a :MetadataRecord ; dct:source ?datasource .
        ?datasource skos:prefLabel ?label .
        FILTER (lang(?label) = 'en')
    } """
   sparql.setQuery(PREFIXES + source_query)
   results = JSON2Polars(sparql.query().convert())
   if save is not None:
      results.write_parquet(save)
   if return_table:
      return results


def query_number_of_letters_and_actors(sparql, return_table=True, save=None):
  # Query datasources and number of letters
  # " \\\(Word files\\\)" causes SyntaxWarning, but SPARQLWrapper needs the third \ for some reason

  letter_query = """
  SELECT DISTINCT ?datasource (COUNT(DISTINCT ?evt) AS ?Letters) WHERE {
  ?evt a :Letter .
  ?evt dct:source/skos:prefLabel ?_datasource .
  FILTER (LANG(?_datasource)="en")
  BIND(REPLACE(STR(?_datasource), " \\\(Word files\\\)", "") AS ?datasource)
  } GROUPBY ?datasource
  """
  sparql.setQuery(PREFIXES + letter_query)
  num_letters = JSON2Polars(sparql.query().convert())
  num_letters = num_letters.group_by('datasource').agg(pl.col("Letters").sum())

  actor_query = """
  SELECT DISTINCT  ?datasource (COUNT(DISTINCT ?person) AS ?Actors)
  WHERE {
  ?person a :ProvidedActor .
  ?person1 :proxy_for ?person .
  ?person1 dct:source/skos:prefLabel ?_datasource .
  FILTER (LANG(?_datasource)="en")
  BIND(REPLACE(STR(?_datasource), " \\\(Word files\\\)", "") AS ?datasource)
  } GROUPBY ?datasource
  """

  sparql.setQuery(PREFIXES + actor_query)
  num_actors = JSON2Polars(sparql.query().convert())
  num_actors = num_actors.group_by('datasource').agg(pl.col("Actors").sum())

  df_numbers = num_letters.join(num_actors, on='datasource').sort("Letters", descending=True)
  
  if save is not None:
    df_numbers.write_parquet(save)
  if return_table:
    return df_numbers


def query_letters_by_source(sparql, source, return_table=True, save=None):
  letter_query = """
    SELECT DISTINCT ?evt ?date ?datasource ?fonds ?year ?source ?target
    (GROUP_CONCAT(DISTINCT ?_sending_place; separator=";") AS ?sending_place)
    (GROUP_CONCAT(DISTINCT ?_target_place; separator=";") AS ?target_place)
    (GROUP_CONCAT(DISTINCT ?_language; separator=";") AS ?language) WHERE {
      ?evt a :Letter .
      ?evt dct:source <<SOURCE>> .
      <<SOURCE>> skos:prefLabel ?datasource .
      FILTER (LANG(?datasource)="en")
      OPTIONAL {?evt :was_addressed_to/:proxy_for ?target}
      OPTIONAL {?evt :was_authored_by/:proxy_for ?source}
      #OPTIONAL {?target skos:prefLabel ?target_label}
      #OPTIONAL {?source skos:prefLabel ?source_label}
      OPTIONAL {?evt :metadata/:sending_date ?date}
      OPTIONAL {?evt :was_sent_from/skos:prefLabel ?_sending_place}
      OPTIONAL {?evt :was_sent_to/skos:prefLabel ?_target_place}
      OPTIONAL {?evt dct:language ?_language}
      OPTIONAL {?evt :fonds ?fonds}
      #OPTIONAL {?evt :archival_organization ?archival_org}
      OPTIONAL {?evt :estimated_year ?year}
    } GROUP BY ?evt ?date ?datasource ?fonds ?year ?source ?target
    """.replace("<SOURCE>", source)

  sparql.setQuery(PREFIXES + letter_query)
  df_source_letters = JSON2Polars(sparql.query().convert())

  if save is not None:
    df_source_letters.write_parquet(save)
  if return_table:
    return df_source_letters
  
def convert_types(df):
  for col in df.columns:
    if df[col].dtype != pl.String:
      df = df.with_columns(df[col].cast(pl.String))
  return df


def query_all_letters(sparql, return_table=True, save=None):
  dfs = []
  sources = query_datasources(sparql)["datasource"].to_list()
  for s in sources:
    dfs.append(convert_types(query_letters_by_source(sparql, s)))
  
  combined_df = dfs[0]
  for df in dfs[1:]:
    combined_df = pl.concat([combined_df,df], how="diagonal")
  
  if save is not None:
    combined_df.write_parquet(save)
  if return_table:
    return combined_df
  

def query_actor_info(sparql):
  letter_query = """
  SELECT DISTINCT ?person
    (SAMPLE(?_label) AS ?label)
    (SAMPLE(?_type) AS ?type)
    (SAMPLE(?_birthyear) AS ?birthyear)
    (SAMPLE(?_deathyear) AS ?deathyear)
    (SAMPLE(?_birthplace) AS ?birthplace)
    (SAMPLE(?_deathplace) AS ?deathplace)
    #(GROUP_CONCAT(DISTINCT ?_source; separator=";") AS ?sources)
    (GROUP_CONCAT(DISTINCT ?_occ; separator=";") AS ?occupations)
    WHERE {
      ?person a :ProvidedActor .
      ?person1 :proxy_for ?person .
      #?person1 dct:source/skos:prefLabel ?_source .
      OPTIONAL { ?person1 skos:prefLabel ?_label}
      OPTIONAL { ?person1 rdf:type/skos:prefLabel ?_type}
      OPTIONAL { ?person1 :birthDate/crm:P82a_begin_of_the_begin ?birthdate . BIND(year(?birthdate) AS ?_birthyear) }
      OPTIONAL { ?person1 :deathDate/crm:P82b_end_of_the_end  ?deathdate . BIND(year(?deathdate) AS ?_deathyear) }
      OPTIONAL { ?person1 :was_born_in_location/skos:prefLabel ?_birthplace}
      OPTIONAL { ?person1 :died_at_location/skos:prefLabel ?_deathplace}
      OPTIONAL { ?person  ^:proxy_for/bioc:has_occupation/skos:prefLabel ?_occ . FILTER(LANG(?_occ)='en')}
    } GROUP BY ?person
"""

  sparql.setQuery(PREFIXES + letter_query)
  person_data = JSON2Polars(sparql.query().convert())
  return person_data

def query_provided_actor_info(sparql):
  query = """
  SELECT DiSTINCT ?person ?out_degree ?in_degree ?num_correspondences ?floruit
    (SAMPLE(?_gender) AS ?gender)
    (GROUP_CONCAT(DISTINCT ?_source; separator=";") AS ?sources)
    (GROUP_CONCAT(DISTINCT ?link; separator=";") AS ?links)
    WHERE {
      ?person a :ProvidedActor .
      OPTIONAL {?person ^:proxy_for/dct:source/skos:prefLabel ?_source .}
      FILTER(LANG(?_source)='en')
      OPTIONAL {?person :out_degree ?out_degree}
      OPTIONAL {?person :in_degree ?in_degree}
      OPTIONAL {?person :num_correspondences ?num_correspondences}

      OPTIONAL { ?person bioc:has_gender/skos:prefLabel ?_gender
        FILTER(LANG(?_gender)='en')
        }
      OPTIONAL { ?person owl:sameAs ?link }
      OPTIONAL {?person :floruit ?floruit }
  } GROUP BY ?person ?out_degree ?in_degree ?num_correspondences ?floruit
  """
  sparql.setQuery(PREFIXES + query)
  person_data = JSON2Polars(sparql.query().convert())
  return person_data

def query_actors(sparql, return_table=True, save=None):
  df1 = query_actor_info(sparql)
  df1 = df1.join(query_provided_actor_info(sparql), on="person")
  if save is not None:
    df1.write_parquet(save)
  if return_table:
    return df1
  

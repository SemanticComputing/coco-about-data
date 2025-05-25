
from datetime import datetime
import re
import polars as pl

import rdflib as rdflib
from rdflib.namespace import XSD, RDF, RDFS, Namespace, SKOS, OWL
from SPARQLWrapper import SPARQLWrapper, JSON, POST, N3, RDFXML

from getpass import getpass

############ GET CREDENTIALS   ############

user = input("Username: ")
password = getpass("Password: ")

############ COCO PREFIXES, ENDPOINT AND CONSTANTS  ############

ENDPOINT = "http://ldf.fi/coco/sparql"

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

sparql = SPARQLWrapper(ENDPOINT)
sparql.setReturnFormat(JSON)
sparql.setMethod(POST)
sparql.setCredentials(user, password)

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

def query_datasources():
   source_query = """
    SELECT DISTINCT ?datasource ?label
    WHERE {
    [] a :MetadataRecord ; dct:source ?datasource .
        ?datasource skos:prefLabel ?label .
        FILTER (lang(?label) = 'en')
    } """
   sparql.setQuery(PREFIXES + source_query)
   results = sparql.query().convert()
   return JSON2Polars(results)


def query_number_of_letters_and_actors():
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
    num_letters = num_letters.groupby('datasource').sum().reset_index()

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
    num_actors = num_actors.groupby('datasource').sum().reset_index()

    df_numbers = num_letters.join(num_actors, on='datasource').sort("Letters", descending=True)
    return df_numbers

df = query_number_of_letters_and_actors()
print(df)


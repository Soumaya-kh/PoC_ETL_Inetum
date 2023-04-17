# Objectif(s)

 
Ce wiki décrit les travaux réalisés dans le cadre de la montée en compétence sur les solutions Cloud Data Fusion et Cloud Dataflow sur GCP.
L'objectif macro est d'éprouver ces solutions afin d'acquérir suffisamment de recul sur leurs capacités et limites, notamment vis-à-vis d'autres solutions (on-premise et sur cloud), et selon les différents cas d'usage qui peuvent exister (en fonction des diverses situations clients).

# Définition du cas d'usage
Pour expérimenter ces deux solutions et les confronter, un cas d'usage est identifié ci-dessous. Il vise à réaliser un ETL sur une table à l'échelle (plusieurs dizaines de To).
- La Table : la table retenue est disponible dans les tables publiques BigQuery. Il s'agit de la table gdeltv2.webngrams. Elle recense toutes les séquences de mots publiées sur internet, ainsi que le contexte associé. Elle fait environ 70 To.
- L'ETL : il s'agit de filtrer la table dans un premier temps sur les séquences de mots en langue anglaise. Dans un deuxième temps, opérer une transformation/regroupement par séquence de mot et par date de manière à pouvoir suivre dans le temps la fréquence d'apparition des séquences de mots

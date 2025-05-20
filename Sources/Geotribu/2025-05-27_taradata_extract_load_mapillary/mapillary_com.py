import gard.taradata.airflow.extensions.connection
import gard.taradata.airflow.schedule as schedule
import gard.taradata.airflow.tasks.dags.sensors as dags_sensors
import gard.taradata.airflow.tasks.database.postgresql_tasks as postgresql_tasks
import gard.taradata.constants.tags as tags
import gard.taradata.helpers.web.http_helper as http_helper
import json

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from datetime import datetime
from gard.taradata.airflow.handlers.cursor import fetch_all_dict_handler
from gard.taradata.data.storage.ogr import *

dag_doc_md = """
Extraction et chargement des "features" détectées par l'IA de [Mapillary](https://www.mapillary.com).
Le code s'appuie sur l'[API map feature](https://www.mapillary.com/developer/api-documentation?locale=fr_FR#map-feature).
"""

@dag(dag_id = "extraction_et_chargement__mensuel__mapillary_com",
     start_date = datetime(1993, 1, 10),
     schedule_interval = schedule.get_dag_cron(tags.extract_and_load, tags.monthly),
     tags = [tags.extract_and_load, tags.monthly],
     catchup = False,
     doc_md = dag_doc_md)
def dag():

    mapillary_conn = Connection.get_connection_from_secrets("Mapillary")

    taradata_conn = Connection.get_connection_from_secrets("Taradata")
    taradata_storage = taradata_conn.to_postgresql_data_storage()

    target_schema = "src_mapillary_com"

    extract_load_tasks_count = 8

    # ---------------------------------------------------------------------------------------------
    # Tâche de synchronisation inter-DAGs.
    # ---------------------------------------------------------------------------------------------

    # ignore l'extraction/chargement si un DAG subséquent (transformation, export) est en cours
    skip_if_subsequent_dag_active = dags_sensors.dags_with_tags_inactive.override(
        task_id = "ignorer_si_dag_subséquent_en_cours",
        timeout = 0,
        soft_fail = True
    )({tags.transform, tags.export})

    # ---------------------------------------------------------------------------------------------
    # Tâche de création du schéma.
    # ---------------------------------------------------------------------------------------------

    create_schema_task = postgresql_tasks.create_schema(taradata_storage, target_schema)

    skip_if_subsequent_dag_active >> create_schema_task

    # ---------------------------------------------------------------------------------------------
    # Tâche de création d'une table temporaire de chargement.
    # ---------------------------------------------------------------------------------------------

    create_table_task = postgresql_tasks.execute_sql_statement.override(task_id = "créer_table_temporaire")(
        taradata_storage,
        """
        drop table if exists tmp_features;

        create table tmp_features(
            geom geometry(polygon, 4326) primary key,
            id_tache integer not null,
            informations jsonb
        );
        """,
        search_path = f"{target_schema},public"
    )

    create_schema_task >> create_table_task

    # ---------------------------------------------------------------------------------------------
    # Tâche de détermination des cellules d'extraction des "features".
    # ---------------------------------------------------------------------------------------------

    # calcul des cellules d'extraction au regard du référentiel routier départemental
    # les cellules sont réparties aléatoirement dans n groupes ou n est égal à extract_load_tasks_count
    compute_cells_task = postgresql_tasks.execute_sql_statement.override(task_id = "déterminer_cellules_extraction")(
        taradata_storage,
        """
        begin;

        create temporary table troncons_wgs84 on commit drop as
        select ST_Transform(geom, 4326) as geom
        from troncons
        where not fictif
        and (siren_proprietaire = '223000019' or siren_gestion_courante = '223000019');

        create index on troncons_wgs84 using gist (geom);

        with emprise as (
            select ST_Collect(geom) as geom
            from troncons_wgs84
        ),
        cellules as (
            select sg.geom
            from emprise e
            cross join ST_SquareGrid(0.01, e.geom) sg
            where exists (
                select *
                from troncons_wgs84 t
                where ST_DWithin(sg.geom, t.geom, 0.0001)
            )
        ),
        repartition_aleatoire as (
            select geom, ntile(%(nb_taches)s) over(order by random()) as id_tache
            from cellules
        )
        insert into tmp_features (geom, id_tache)
        select geom, id_tache
        from repartition_aleatoire;

        commit;
        """,
        statement_params = {"nb_taches": extract_load_tasks_count},
        search_path = f"{target_schema},wrh_referentiel_routier,public",
    )

    create_table_task >> compute_cells_task

    # ---------------------------------------------------------------------------------------------
    #  Tâches d'extraction et de chargement des "features" depuis Mapillary.
    # ---------------------------------------------------------------------------------------------

    def get_task_cells(extract_load_task_id: int):
        """
        Récupération de la liste des cellules à extraire et charger par une tâche donnée.

        extract_load_task_id : L'identifiant de la tâche d'extraction et de chargement.
        """
        return postgresql_tasks.execute_sql_statement.function(
            taradata_storage,
            """
            select
                geom,
                ST_XMin(geom) as x_min,
                ST_YMin(geom) as y_min,
                ST_XMax(geom) as x_max,
                ST_YMax(geom) as y_max
            from tmp_features
            where id_tache = %(id_tache)s
            and informations is null
            """,
            statement_params = {"id_tache": extract_load_task_id},
            search_path = f"{target_schema},public",
            handler = fetch_all_dict_handler
        )

    def call_map_features_api(cell: dict):
        """
        Appel à l'API d'extraction des "features" au format JSON pour une cellule donnée.

        cell : La cellule pour laquelle les "features" doivent être extraites.
        """
        url = (
            "https://graph.mapillary.com/map_features"
            f"?access_token={mapillary_conn.password}"
            "&fields=id,aligned_direction,first_seen_at,last_seen_at,object_value,object_type,geometry"
            f"&bbox={cell['x_min']},{cell['y_min']},{cell['x_max']},{cell['y_max']}"
        )

        return http_helper.get_json(url, verify = False)
    
    def divide_task_cells(extract_load_task_id: int):
        """
        Division des cellules non extraites et chargées en cellules de surface 4 fois inférieures.
        
        extract_load_task_id : L'identifiant de la tâche d'extraction et de chargement pour laquelle il faut diviser les cellules.
        """
        postgresql_tasks.execute_sql_statement.function(
            taradata_storage,
            """
            with cellules_a_diviser as (
                delete
                from tmp_features
                where id_tache = %(id_tache)s
                and informations is null
                returning geom
            )
            insert into tmp_features (geom, id_tache)
            select sg.geom, %(id_tache)s
            from cellules_a_diviser cad
            inner join ST_SquareGrid((ST_XMax(cad.geom)::numeric - ST_XMin(cad.geom)::numeric) / 2, cad.geom) sg
            on ST_Intersects(sg.geom, cad.geom) and not ST_Touches(sg.geom, cad.geom)
            """,
            statement_params = {"id_tache": extract_load_task_id},
            search_path = f"{target_schema},public",
        )

    @task(task_id = "extraire_charger_features", retries = 3)
    def extract_load_features(extract_load_task_id: int):
        """
        Tâche d'extraction et de chargement des "features" présentes sur l'emprise de cellules.

        extract_load_task_id : Le numéro de tâche d'extraction et de chargement.
        """
        # récupération de la liste des cellules à extraire/charger
        cells = get_task_cells(extract_load_task_id)

        while (cells):

            # pour chaque cellule :
            for cell in cells:
                
                # appel de l'API
                features = call_map_features_api(cell)

                if (len(features["data"]) < 2000): 
                    # si le résultat contient moins de 2000 éléments (limite de l'API)
                    # alors, chargement du résultat dans la base de données                    
                    postgresql_tasks.execute_sql_statement.function(
                        taradata_storage,
                        """
                        update tmp_features
                        set informations = %(informations)s
                        where ST_Equals(geom, (%(geom)s)::geometry);
                        """,    
                        statement_params = {
                            "informations": json.dumps(features),
                            "geom": cell["geom"] 
                        },
                        search_path = f"{target_schema},public",
                        log_sql = False
                    )

            # division des cellules qui n'ont pas pu être extraites/chargées
            divide_task_cells(extract_load_task_id)

            # et récupération de la nouvelle liste des cellules à extraire/charger
            cells = get_task_cells(extract_load_task_id)

    extract_load_features_tasks = []
    for extract_load_task_id in range(1, extract_load_tasks_count + 1):
        extract_load_features_tasks.append(extract_load_features.override(task_id = f"extraire_charger_features_{extract_load_task_id}")(extract_load_task_id))

    compute_cells_task >> extract_load_features_tasks

    # ---------------------------------------------------------------------------------------------
    # Tâche de remplacement de la table définitive par la table temporaire de chargement.
    # ---------------------------------------------------------------------------------------------

    replace_table_task = postgresql_tasks.execute_sql_statement.override(task_id = "remplacer_table")(
        taradata_storage,
        """
        begin;

        drop table if exists features cascade;
        create table features as select informations from tmp_features;
        drop table tmp_features;

        commit;
        """,
        search_path = target_schema
    )

    extract_load_features_tasks >> replace_table_task

dag()
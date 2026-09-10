# Déploiement Kubernetes — Airflow + Postgres + pgAdmin sur Scaleway

Guide d'installation complète, de zéro, sur le cluster Scaleway Kapsule `dropkick-murphys`.
Namespace cible : `airflow` (partagé par Airflow, les 2 Postgres et les 2 pgAdmin).

## Prérequis

- `kubectl`, `helm`, `docker` (buildx) installés en local
- Le fichier yaml de connexion au cluster à la racine du repo (**ne jamais le committer**)
- Accès à la console Scaleway (registre `keolis-registry`, bucket `keolis-data`, clé API)
- Accès DNS sur la zone `davidson-si-nord.fr` (Route 53)

```bash
export KUBECONFIG=$(pwd)/kubeconfig-dropkick-murphys.yaml
kubectl config current-context 
```

---

## 1. Namespace

```bash
kubectl create namespace airflow
```

---

## 2. DNS

Le cluster est derrière ce LoadBalancer nginx-ingress (déjà en place, partagé avec les autres apps) :

```
195.154.73.172   (195-154-73-172.lb.fr-par.scw.cloud)
```

Créer les enregistrements A suivants vers cette IP, dans la zone `davidson-si-nord.fr` :

| Host | Usage |
|---|---|
| `keolis.airflow.davidson-si-nord.fr` | UI Airflow |
| `keolis.db1.davidson-si-nord.fr` | pgAdmin — base source (db1) |
| `keolis.db2.davidson-si-nord.fr` | pgAdmin — base destination (db2) |

Sans ces enregistrements, `cert-manager` ne pourra pas valider le challenge HTTP-01 et les
certificats Let's Encrypt resteront `Pending`.

---

## 3. Registre de conteneurs Scaleway (optionnel selon l'image utilisée)

Le déploiement actuel utilise l'image officielle **`apache/airflow:3.2.2`** (multi-arch,
Docker Hub public) pour les composants Airflow — pas d'image custom, donc pas besoin
d'identifiants de registre pour *ces* pods. Le secret est quand même créé/conservé car
`deployment/values.yaml` le référence (`imagePullSecrets`) et il sert si un jour une image
custom (`rg.fr-par.scw.cloud/keolis-registry/airflow-custom`) est repoussée en usage :

```bash
kubectl create secret docker-registry scw-registry-credentials -n airflow --docker-server=rg.fr-par.scw.cloud --docker-username=<SCW_ACCESS_KEY> --docker-password='<SCW_SECRET_KEY>'
```

> Si vous rebuildez une image custom un jour : buildez avec `--platform linux/amd64`
> (les nœuds du cluster sont en amd64, une image buildée sur un Mac Apple Silicon sans ce
> flag donne `exec /usr/bin/dumb-init: exec format error` au démarrage des pods).
> ```bash
> docker buildx build --platform linux/amd64 -t rg.fr-par.scw.cloud/keolis-registry/airflow-custom:<tag> -f dockerfile --push .
> ```

---

## 4. Secret — logs Airflow distants vers le bucket S3 Scaleway

```bash
kubectl create secret generic airflow-s3-conn -n airflow --from-literal=connection="aws://<SCW_ACCESS_KEY>:<SCW_SECRET_KEY>@/?region_name=fr-par&endpoint_url=https%3A%2F%2Fs3.fr-par.scw.cloud"
```

Consommé par `config.logging` + `extraEnv` dans `deployment/values.yaml` (connexion Airflow
`aws_default`, logs écrits sous `s3://keolis-data/airflow-logs`).

---

## 5. Secret — mot de passe admin Airflow

```bash
kubectl create secret generic airflow-admin-credentials -n airflow --from-literal=username=admin --from-literal=password='<UN_MOT_DE_PASSE_FORT>'
```

Consommé par le bloc `createUserJob` de `deployment/values.yaml` (job rejoué à chaque
`helm upgrade` ; il tente `airflow users create` puis force `airflow users reset-password`,
donc idempotent même si l'utilisateur existe déjà).

---

## 6. Déployer Airflow

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow -n airflow -f deployment/values.yaml
```

`deployment/values.yaml` configure : `KubernetesExecutor`, l'image `apache/airflow:3.2.2`,
les logs distants S3, l'admin via secret, et l'ingress (`ingress.apiServer` — spécifique à
Airflow 3+, ne pas confondre avec `ingress.web` qui est pour Airflow <3.0.0) sur
`keolis.airflow.davidson-si-nord.fr` avec TLS `cert-manager`/`letsencrypt-production`.

Vérifier :
```bash
kubectl get pods -n airflow
kubectl get ingress -n airflow
kubectl describe certificate airflow-tls -n airflow
```

> Un `airflow-triggerer-0` (StatefulSet) qui reste en `Init:CrashLoopBackOff` après un
> changement d'image signifie que le pod n'a pas été recréé automatiquement — forcer avec
> `kubectl -n airflow delete pod airflow-triggerer-0`.

---

## 7. Postgres db1 (source) + db2 (destination) + pgAdmin

Manifests dans `deployment/postgres/db1.yaml` et `db2.yaml` (aucun secret en clair dedans).

### 7.1 Secrets — connexion Postgres (lus automatiquement par les pods de pipeline Airflow)

```bash
kubectl create secret generic src-postgres-credentials -n airflow --from-literal=SRC_POSTGRES_HOST=postgres-db1 --from-literal=SRC_POSTGRES_PORT=5432 --from-literal=SRC_POSTGRES_USER=user_db1 --from-literal=SRC_POSTGRES_PASSWORD='<MDP_DB1>' --from-literal=SRC_POSTGRES_DATABASE=ma_base_1
```
```bash
kubectl create secret generic dst-postgres-credentials -n airflow --from-literal=DST_POSTGRES_HOST=postgres-db2 --from-literal=DST_POSTGRES_PORT=5432 --from-literal=DST_POSTGRES_USER=user_db2 --from-literal=DST_POSTGRES_PASSWORD='<MDP_DB2>' --from-literal=DST_POSTGRES_DATABASE=ma_base_2
```

Ce sont exactement les clés attendues par `required_env_vars()`
(`scripts/pipeline_config.py`) et `SECRET_NAME_BY_PREFIX`
(`dags/dlt_pipeline_operator.py`) : tout DAG V2 avec `credentials_env_prefix: SRC_POSTGRES`
ou `DST_POSTGRES` recevra ces variables automatiquement dans son pod.

### 7.2 Secrets — pgAdmin (login UI + mot de passe DB pré-rempli)

⚠️ L'email pgAdmin doit être un domaine "réel" — un TLD réservé comme `.local` est rejeté
par le validateur au démarrage (le pod part en `Error`).

```bash
kubectl create secret generic pgadmin-db1-credentials -n airflow --from-literal=email=pgadmin-db1@davidson-si-nord.fr --from-literal=password='<MDP_PGADMIN_DB1>'
```
```bash
kubectl create secret generic pgadmin-db1-pgpass -n airflow --from-literal=pgpass='postgres-db1:5432:ma_base_1:user_db1:<MDP_DB1>'
```
```bash
kubectl create secret generic pgadmin-db2-credentials -n airflow --from-literal=email=pgadmin-db2@davidson-si-nord.fr --from-literal=password='<MDP_PGADMIN_DB2>'
```
```bash
kubectl create secret generic pgadmin-db2-pgpass -n airflow --from-literal=pgpass='postgres-db2:5432:ma_base_2:user_db2:<MDP_DB2>'
```

### 7.3 Déployer

```bash
kubectl apply -f deployment/postgres/db1.yaml
```
```bash
kubectl apply -f deployment/postgres/db2.yaml
```

### 7.4 Vérifier

```bash
kubectl get pods -n airflow -l 'app in (postgres-db1,postgres-db2,pgadmin-db1,pgadmin-db2)'
```
```bash
kubectl get ingress -n airflow
```
```bash
kubectl describe certificate pgadmin-db1-tls pgadmin-db2-tls -n airflow
```

Puis ouvrir `https://keolis.db1.davidson-si-nord.fr` et
`https://keolis.db2.davidson-si-nord.fr` — se connecter avec l'email/password pgAdmin de
l'étape 7.2 (pas les identifiants Postgres). Le serveur (`db1 (source)` / `db2
(destination)`) doit apparaître déjà connecté dans l'arborescence, grâce au `pgpass` monté.

---

## Récapitulatif des secrets à créer (namespace `airflow`)

| Secret | Contenu | Consommé par |
|---|---|---|
| `scw-registry-credentials` | docker-registry auth | `imagePullSecrets` (chart Airflow) |
| `airflow-s3-conn` | connection URI S3 Scaleway | logs distants Airflow (`aws_default`) |
| `airflow-admin-credentials` | `username`, `password` | `createUserJob` (chart Airflow) |
| `src-postgres-credentials` | `SRC_POSTGRES_{HOST,PORT,USER,PASSWORD,DATABASE}` | pods de pipeline dlt + `postgres-db1` |
| `dst-postgres-credentials` | `DST_POSTGRES_{HOST,PORT,USER,PASSWORD,DATABASE}` | pods de pipeline dlt + `postgres-db2` |
| `pgadmin-db1-credentials` | `email`, `password` | login UI pgAdmin db1 |
| `pgadmin-db1-pgpass` | fichier `.pgpass` | connexion auto pgAdmin → db1 |
| `pgadmin-db2-credentials` | `email`, `password` | login UI pgAdmin db2 |
| `pgadmin-db2-pgpass` | fichier `.pgpass` | connexion auto pgAdmin → db2 |

Aucun de ces secrets n'est versionné dans le repo — uniquement les manifestes qui les
référencent (`deployment/values.yaml`, `deployment/postgres/*.yaml`).

## Sécurité — à garder en tête

- `keolis.airflow.*`, `keolis.db1.*` et `keolis.db2.*` sont **publiquement accessibles**
  (certificats Let's Encrypt = domaines publics indexables). L'authentification protège
  l'accès mais une restriction IP (`nginx.ingress.kubernetes.io/whitelist-source-range`)
  ou un accès VPN est recommandé pour pgAdmin en particulier (accès direct aux données).
- Le cluster est **partagé** avec d'autres projets (namespaces `keolis`, `davmom-uat`,
  `metabase`, etc.) — rester dans le namespace `airflow` pour toute nouvelle ressource.

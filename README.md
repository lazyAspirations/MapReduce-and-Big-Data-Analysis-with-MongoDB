# TP4 - Missions Pratiques : Challenge des Data Engineers

## Objectif
Solution complète pour la section 5 du TP : Missions A, B et C avec MongoDB. Ce dossier propose :
- Scripts `mission_a_insights.js`, `mission_b_quality.js`, `mission_c_structure.js`
- Instructions d'importation de données Kaggle
- Commandes de lancement depuis `mongosh`

## Prérequis
- MongoDB installé et démarré
- `mongosh` disponible
- Dossiers contenant les fichiers téléchargés depuis Kaggle
- Base de données MongoDB : `universite` (ou adaptez le nom)

## Import des données Kaggle
### Mission A (Insights & Tendances)
Dataset conseillé : `E-commerce Sales Data` / `Online Retail Dataset`

Exemple :
```powershell
mongoimport --db universite --collection sales_logs --type csv --headerline --file C:\chemin\vers\sales_data.csv
```

### Mission B (Qualité & Transformation)
Dataset conseillé : un dataset hétérogène/"dirty" avec champs mixtes et valeurs manquantes, par exemple `Messy Retail Dataset` ou une table de sondage publique.

Exemple :
```powershell
mongoimport --db universite --collection dirty_data --type csv --headerline --file C:\chemin\vers\dirty_data.csv
```

### Mission C (Structure & Corrélation)
Dataset conseillé : une paire `transactions + users` ou `orders + products`.

Exemples :
```powershell
mongoimport --db universite --collection transactions --type csv --headerline --file C:\chemin\vers\transactions.csv
mongoimport --db universite --collection users --type csv --headerline --file C:\chemin\vers\users.csv
```

> Important : adaptez le nom des champs dans les scripts au schéma réel de votre dataset.

### Import automatique des fichiers fournis
Vous pouvez aussi exécuter directement le script Python d’import depuis le dossier `Sol` :
```powershell
python .\import_csvs.py
```
Ce script charge automatiquement :
- `pet_supplies.csv` → collection `sales_logs`
- `customer_shopping_behavior.csv` → collections `dirty_data` et `transactions`
- `01_company_info.csv` → collection `companies`

## Étape par étape : préparation et exécution

### 1) Télécharger un dataset Kaggle
- Mission A : choisissez un dataset de ventes, e-commerce ou tendances produit.
- Mission B : choisissez un dataset "dirty" avec valeurs manquantes et types mixtes.
- Mission C : choisissez une paire de datasets liés, par exemple transactions + utilisateurs.

> Exemple : `online retail`, `ecommerce sales data`, `customer transactions + customer profiles`.

### 2) Vérifier MongoDB
1. Assurez-vous que MongoDB est installé.
2. Lancez le serveur MongoDB :
```powershell
mongod --dbpath C:\data\db
```
3. Ouvrez un autre terminal PowerShell pour exécuter `mongosh`.

### 3) Importer les fichiers Kaggle dans MongoDB
Remplacez `C:\chemin\vers\...` par le chemin réel de vos fichiers téléchargés.

Mission A :
```powershell
mongoimport --db universite --collection sales_logs --type csv --headerline --file C:\chemin\vers\sales_data.csv
```
Mission B :
```powershell
mongoimport --db universite --collection dirty_data --type csv --headerline --file C:\chemin\vers\dirty_data.csv
```
Mission C :
```powershell
mongoimport --db universite --collection transactions --type csv --headerline --file C:\chemin\vers\transactions.csv
mongoimport --db universite --collection users --type csv --headerline --file C:\chemin\vers\users.csv
```

Si le dataset est en JSON :
```powershell
mongoimport --db universite --collection sales_logs --file C:\chemin\vers\sales_data.json --jsonArray
```

### 4) Vérifier les champs importés
Après import, ouvrez `mongosh` et exécutez :
```js
use universite
show collections
```
Puis pour chaque collection :
```js
db.sales_logs.findOne()
db.dirty_data.findOne()
db.transactions.findOne()
db.users.findOne()
```

### 5) Adapter les scripts aux noms de champs
- Si votre dataset utilise un champ `price_usd` au lieu de `price`, remplacez-le dans `mission_b_quality.js`.
- Si votre dataset utilise `productCategory` au lieu de `category`, remplacez-le dans `mission_a_insights.js`.
- Si la clé de jointure est `customer_id` et non `user_id`, modifiez `localField`/`foreignField` dans `mission_c_structure.js`.

### 6) Exécuter les scripts
Depuis le dossier `Sol` :
```powershell
mongosh --file .\mission_a_insights.js
mongosh --file .\mission_b_quality.js
mongosh --file .\mission_c_structure.js
```

## Notes utiles
- `allowDiskUse: true` est inclus pour éviter les erreurs de mémoire sur pipelines lourds.
- Les scripts convertissent les champs texte en numériques avec `$convert`.
- Si un champ n'existe pas, adaptez le script ou utilisez `db.collection.findOne()` pour trouver le bon champ.

---

## Résumé des missions
- Mission A : filtre temporel/catégoriel, agrégation, top 10
- Mission B : nettoyage, conversion, calculs de synthèse, sauvegarde avec `$out`
- Mission C : jointure entre deux collections, `unwind`, statistiques multidimensionnelles

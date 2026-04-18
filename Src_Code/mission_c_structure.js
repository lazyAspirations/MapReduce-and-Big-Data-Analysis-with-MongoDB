// Mission C - Pôle "Structure & Corrélation"
// Objectif : Croiser deux collections pour obtenir une analyse multidimensionnelle.

const db = connect("127.0.0.1:27017/universite");
const orders = db.transactions;
const companies = db.companies;

const pipeline = [
  {
    $match: {
      location: { $exists: true, $ne: "" },
      purchase_amount_usd: { $gt: 0 }
    }
  },
  {
    $lookup: {
      from: "companies",
      localField: "location",
      foreignField: "city",
      as: "company_info"
    }
  },
  { $unwind: "$company_info" },
  {
    $group: {
      _id: {
        industry: "$company_info.industry",
        category: "$category"
      },
      totalRevenue: { $sum: "$purchase_amount_usd" },
      orderCount: { $sum: 1 },
      avgOrderValue: { $avg: "$purchase_amount_usd" }
    }
  },
  { $sort: { totalRevenue: -1 } },
  { $limit: 20 }
];

const lookupStart = new Date();
const result = orders.aggregate(pipeline, { allowDiskUse: true }).toArray();
const lookupEnd = new Date();
print("Temps d'exécution pour analyse croisée avec jointure : " + (lookupEnd - lookupStart) + " ms");
print('--- Mission C : Analyse croisée industry x catégorie ---');
printjson(result);

const noLookupPipeline = [
  {
    $match: {
      purchase_amount_usd: { $gt: 0 }
    }
  },
  {
    $group: {
      _id: "$category",
      totalRevenue: { $sum: "$purchase_amount_usd" },
      orderCount: { $sum: 1 }
    }
  },
  { $sort: { totalRevenue: -1 } },
  { $limit: 20 }
];

const noLookupStart = new Date();
const noLookupResult = orders.aggregate(noLookupPipeline, { allowDiskUse: true }).toArray();
const noLookupEnd = new Date();
print("Temps d'exécution pour analyse sans jointure : " + (noLookupEnd - noLookupStart) + " ms");
print('--- Mission C : Analyse sans jointure (comparaison de performance) ---');
printjson(noLookupResult);

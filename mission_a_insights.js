// Mission A - Pôle "Insights & Tendances"
// Objectif : Identifier les tendances dominantes (Top 10 produits) à partir du dataset pet_supplies.

const db = connect("127.0.0.1:27017/universite");
const coll = db.sales_logs;

const pipeline = [
  {
    $addFields: {
      quantity: { $convert: { input: "$quantity", to: "int", onError: 0, onNull: 0 } },
      averageStar: { $convert: { input: "$averageStar", to: "double", onError: 0, onNull: 0 } },
      wishedCount: { $convert: { input: "$wishedCount", to: "int", onError: 0, onNull: 0 } }
    }
  },
  {
    $group: {
      _id: "$title",
      totalQuantity: { $sum: "$quantity" },
      averageRating: { $avg: "$averageStar" },
      totalWished: { $sum: "$wishedCount" },
      count: { $sum: 1 }
    }
  },
  { $sort: { totalQuantity: -1 } },
  { $limit: 10 }
];

const start = new Date();
const result = coll.aggregate(pipeline, { allowDiskUse: true }).toArray();
const end = new Date();
print("Temps d'exécution pour top 10 produits par quantité : " + (end - start) + " ms");
print('--- Mission A : Top 10 produits par quantité vendue ---');
printjson(result);

const ratingPipeline = [
  {
    $addFields: {
      quantity: { $convert: { input: "$quantity", to: "int", onError: 0, onNull: 0 } },
      averageStar: { $convert: { input: "$averageStar", to: "double", onError: 0, onNull: 0 } }
    }
  },
  {
    $group: {
      _id: "$title",
      averageRating: { $avg: "$averageStar" },
      totalQuantity: { $sum: "$quantity" }
    }
  },
  { $sort: { averageRating: -1, totalQuantity: -1 } },
  { $limit: 10 }
];

const ratingStart = new Date();
const ratingResult = coll.aggregate(ratingPipeline, { allowDiskUse: true }).toArray();
const ratingEnd = new Date();
print("Temps d'exécution pour top 10 produits par note moyenne : " + (ratingEnd - ratingStart) + " ms");
print('--- Mission A : Top 10 produits par note moyenne ---');
printjson(ratingResult);

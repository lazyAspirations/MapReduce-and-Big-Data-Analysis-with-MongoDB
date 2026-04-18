// Mission B - Pôle "Qualité & Transformation"
// Objectif : Nettoyer les données hétérogènes et sauvegarder le résultat propre.

const db = connect("127.0.0.1:27017/universite");
const coll = db.dirty_data;

const pipeline = [
  {
    $addFields: {
      purchase_amount_usd: {
        $convert: { input: "$purchase_amount_usd", to: "double", onError: null, onNull: null }
      },
      review_rating: {
        $convert: { input: "$review_rating", to: "double", onError: null, onNull: null }
      },
      age: {
        $convert: { input: "$age", to: "int", onError: null, onNull: null }
      },
      previous_purchases: {
        $convert: { input: "$previous_purchases", to: "int", onError: null, onNull: null }
      }
    }
  },
  {
    $match: {
      purchase_amount_usd: { $gt: 0 },
      category: { $exists: true, $ne: "" },
      item_purchased: { $exists: true, $ne: "" }
    }
  },
  {
    $group: {
      _id: null,
      averagePurchase: { $avg: "$purchase_amount_usd" },
      totalRevenue: { $sum: "$purchase_amount_usd" },
      salesCount: { $sum: 1 },
      averageRating: { $avg: "$review_rating" },
      averageAge: { $avg: "$age" }
    }
  },
  {
    $project: {
      _id: 0,
      averagePurchase: 1,
      totalRevenue: 1,
      salesCount: 1,
      averageRating: 1,
      averageAge: 1
    }
  }
];

const statsStart = new Date();
const stats = coll.aggregate(pipeline, { allowDiskUse: true }).toArray();
const statsEnd = new Date();
print("Temps d'exécution pour statistiques des données nettoyées : " + (statsEnd - statsStart) + " ms");
print('--- Mission B : Statistiques des données nettoyées ---');
printjson(stats);

const cleanupPipeline = [
  {
    $addFields: {
      purchase_amount_usd: {
        $convert: { input: "$purchase_amount_usd", to: "double", onError: null, onNull: null }
      },
      review_rating: {
        $convert: { input: "$review_rating", to: "double", onError: null, onNull: null }
      },
      age: {
        $convert: { input: "$age", to: "int", onError: null, onNull: null }
      },
      previous_purchases: {
        $convert: { input: "$previous_purchases", to: "int", onError: null, onNull: null }
      }
    }
  },
  {
    $match: {
      purchase_amount_usd: { $gt: 0 },
      category: { $exists: true, $ne: "" },
      item_purchased: { $exists: true, $ne: "" }
    }
  },
  {
    $project: {
      customer_id: 1,
      age: 1,
      gender: 1,
      item_purchased: 1,
      category: 1,
      purchase_amount_usd: 1,
      location: 1,
      size: 1,
      color: 1,
      season: 1,
      review_rating: 1,
      subscription_status: 1,
      shipping_type: 1,
      discount_applied: 1,
      previous_purchases: 1,
      payment_method: 1,
      frequency_of_purchases: 1
    }
  },
  {
    $out: "cleaned_data"
  }
];

print('--- Mission B : Exécution du pipeline de nettoyage et sauvegarde dans cleaned_data ---');
const cleanupStart = new Date();
coll.aggregate(cleanupPipeline, { allowDiskUse: true });
const cleanupEnd = new Date();
print("Temps d'exécution pour nettoyage et sauvegarde : " + (cleanupEnd - cleanupStart) + " ms");
print('Collection cleaned_data créée / mise à jour.');

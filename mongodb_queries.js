
// Query 1

[ 
  { $group: { 
      _id: "$release_year",
      avg_rating: { $avg: "$rating" },
      avg_budget: { $avg: "$production_budget" } 
    }},
  { $match: { avg_rating: { $gte: 3 } }},
  { $sort: { avg_budget: -1 } }
]


// Query 2
[
  {
    $unwind: "$genre"
  },
  {
    $group: {
      _id: "$genre",
      total_views: {
        $sum: "$views_count"
      }
    }
  },
  {
    $sort: {
      total_views: -1
    }
  },
  {
    $limit: 3
  }
]


// Query 3

[
  {
    $group: {
      _id: "$genre",
      total_views: { $sum: "$total_views" },
      avg_budget: { $avg: "$production_budget" }
    }
  },
  {
    $match: {
      total_views: { $gte: 100000 },
      avg_budget: { $gte: 40000000 }
    }
  },
  {
    $sort: {
      total_views: -1
    }
  }
]

// Query 4

[
  {
    "$project": {
      "title": 1,
      "seasons": 1,
      "episodes_per_season": 1,
      "total_episodes": { "$sum": "$episodes_per_season" }
    }
  },
  {
    "$match": {
      "seasons": { "$gte": 5 },
      "total_episodes": { "$gte": 50 }
    }
  },
  {
    "$sort": {
      "total_episodes": -1
    }
  }
]
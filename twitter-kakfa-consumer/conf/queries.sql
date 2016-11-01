--NBM_visualization
SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=predicted and gt=0) OR (gt=4 and predicted=0)) as PRECISIONE
FROM test_NBM AS C0
WHERE gt=predicted and gt=0
GROUP BY gt
UNION
SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=predicted and gt=4) OR (gt=0 and predicted=4)) as PRECISIONE
FROM test_NBM AS C1
WHERE gt=predicted and gt=4
GROUP BY gt

SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=predicted and gt=0) OR (gt=0 and predicted=4)) as RECALL
FROM test_NBM AS C0
WHERE gt=predicted and gt=0
GROUP BY gt
UNION
SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=predicted and gt=4) OR (gt=4 and predicted=0)) as RECALL
FROM test_NBM AS C1
WHERE gt=predicted and gt=4
GROUP BY gt

SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=4 and predicted=0) OR (gt=predicted and gt=0) ) as FALSE_DISCOVERY_RATE
FROM test_NBM AS C0
WHERE (gt=4 and predicted=0)
GROUP BY gt
UNION
SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=0 and predicted=4) OR (gt=predicted and gt=4) ) as FALSE_DISCOVERY_RATE
FROM test_NBM AS C0
WHERE (gt=0 and predicted=4)
GROUP BY gt

SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=4 and predicted=0) OR (gt=predicted and gt=4) ) as FALL_OUT
FROM test_NBM AS C0
WHERE (gt=4 and predicted=0)
GROUP BY gt
UNION
SELECT gt,COUNT(*)/(SELECT COUNT(*) FROM test_NBM WHERE (gt=0 and predicted=4) OR (gt=predicted and gt=0) ) as FALL_OUT
FROM test_NBM AS C0
WHERE (gt=0 and predicted=4)
GROUP BY gt

--k-means visualization
SELECT *
FROM test_kmeans;

SELECT gt,predicted,COUNT(*) as C1
FROM test_kmeans
WHERE gt=0
GROUP BY (predicted)
UNION
SELECT gt,predicted,COUNT(*) as C2
FROM test_kmeans
WHERE gt=2
GROUP BY (predicted)
UNION
SELECT gt,predicted,COUNT(*) as C2
FROM test_kmeans
WHERE gt=4
GROUP BY (predicted)



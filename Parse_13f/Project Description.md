# Project Description

1.       Use SEC company master index

          https://www.sec.gov/Archives/edgar/full-index/2018/QTR4/company.idx

2.       Identify all ‘13F-HR’ file location from master index file

3.       Identify hedge fund companies. Instead of relying on vendor maps, we use a naïve way to identify company key words, LP, L.P.,LLP, L.L.P., LLC, L.L.C.

4.       Download each individual 13F holding report and identify cusip and values.

5.       Aggregate all hedge fund holdings and join with R3000 list in Russell3K.csv. We excluded all put and call options in our analysis.

6.       Reporting:

  a.      Top hedge fund ownership stocks
  b.      Largest hedge fund by total stock value

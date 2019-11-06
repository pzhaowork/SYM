import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
from multiprocessing import Process, Manager
import time

#Parsing each hedge fund url with simple error checking condition
def HedgeFundHolding(url, R3000List):
    try:
        soup = BeautifulSoup(requests.get(url).content, 'xml')
    except:
        time.sleep(0.5)
        soup = BeautifulSoup(requests.get(url).content, 'xml')

    #Remove put and call options
    table = [x for x in soup.find_all('infoTable') if 'putCall' not in x]

    #Only process stock information within R3000
    SecurityTable = Counter({i.find('cusip').text: int(i.find('value').text) for i in table if i.find('cusip').text in R3000List})

    #Simple filter: if any R3000 stock held by the hedge fund is more than 40% of market cap, we will assume reporting unit error in $1 not in $1000.
    Filter = [SecurityTable[i]*1000>R3000List[i]*0.40 for i in SecurityTable.keys()]
    if any(Filter):
        SecurityTable = Counter({i: SecurityTable[i]//1000 for i in SecurityTable})
    return SecurityTable, sum(SecurityTable.values())

#sub-job function for multiprocessing, iterating through partitioned idx file and processing each url
def Processing13F(start, end, HedgeFundF13doc, stock, hedgefund, R3000List):
    ProcessedTable = Counter()
    ProcessedHedgeFund = {}
    SecUrl = 'https://www.sec.gov/Archives/'
    for index, row in HedgeFundF13doc[start:end].iterrows():
        url = (SecUrl + row[4])
        EachHedgeFund, TotalStockValue = HedgeFundHolding(url, R3000List)
        ProcessedTable = ProcessedTable + EachHedgeFund
        ProcessedHedgeFund[row[2]] = [row[0], TotalStockValue, url]
        print("Processed 13F for ", index, " ", row[0])
    stock.append(ProcessedTable)
    hedgefund.append(ProcessedHedgeFund)

#Processing master idx file
def CreateHedgeFundList():
    url = 'https://www.sec.gov/Archives/edgar/full-index/2018/QTR4/company.idx'
    lines = requests.get(url).content.decode("utf-8", "ignore").splitlines()
    typeloc = lines[8].find('Form Type')
    cikloc = lines[8].find('CIK')
    dateloc = lines[8].find('Date Filed')
    urlloc = lines[8].find('File Name')
    records = [tuple([line[:typeloc].strip(), line[typeloc:cikloc].strip(), line[cikloc:dateloc].strip(),
                      line[dateloc:urlloc].strip(), line[urlloc:].strip()]) for line in lines[10:]]
    data = pd.DataFrame(records, columns=('Company Name', 'Form Type', 'CIK', 'Date Filed', 'File Name'))
    HedgeFundId = tuple(['LP', 'L.P.', 'LLP', 'L.L.P.', 'LLC', 'L.L.C.'])
    HedgeFundF13doc = data[(data['Form Type'].str.upper() == '13F-HR') & (data['Company Name'].str.upper().str.endswith(HedgeFundId))]
    HedgeFundF13doc.reset_index(drop=True, inplace=True)
    return HedgeFundF13doc

#Download R3000 file
def InitializeR3000():
    return pd.read_csv('./Russell3K.csv')

if __name__ == '__main__':

    timeStart = time.time()

    HedgeFundF13doc = CreateHedgeFundList()
    R3000 = InitializeR3000()
    R3000List = {row[1]: row[2] for index, row in R3000.iterrows()}

    process_count = 12
    process_list = []
    TotalJob = len(HedgeFundF13doc)
    stock = Manager().list()
    hedgefund = Manager().list()

    # break jobs
    for i in range(process_count):
        if i != (process_count - 1):
            start = (i * TotalJob // process_count) + 1
            end = ((i + 1) * TotalJob // process_count) + 1
            process_list.append(
                Process(target=Processing13F, args=(start, end, HedgeFundF13doc, stock, hedgefund, R3000List)))
        else:
            start = (i * TotalJob // process_count) + 1
            end = TotalJob
            process_list.append(
                Process(target=Processing13F, args=(start, end, HedgeFundF13doc, stock, hedgefund, R3000List)))

    # multiprocessing all jobs
    for process in process_list:
        process.start()
    for process in process_list:
        process.join()

    # combining results
    MasterStock = sum(stock, Counter())
    MasterHedgeFund = {}
    for i in hedgefund:
        MasterHedgeFund.update(i)

    MasterStock
    MasterStockTable = pd.DataFrame.from_dict(MasterStock, orient='index', columns=['value'])
    MasterHedgeFundTable = pd.DataFrame.from_dict(MasterHedgeFund, orient='index', columns = ['company name', 'total stock value', 'url'])

    R3000.set_index('Cusip', inplace = True)
    MasterStockTable = MasterStockTable.join(R3000, how = 'left')
    MasterStockTable['ratio'] = MasterStockTable['value']*1000/MasterStockTable['MCap']
    MasterStockTable.index.name = 'cusip'
    MasterHedgeFundTable.index.name = 'CIK'

    MasterStockTable.sort_values(by = 'ratio', ascending = False, inplace = True)
    MasterHedgeFundTable.sort_values(by = 'total stock value', ascending = False, inplace = True)


    MasterStockTable.to_csv('MasterStockTable.csv')
    MasterHedgeFundTable.to_csv('MasterHedgeFundTable.csv')

    timeEnd = time.time()
    print('Process Time (s): ', timeEnd - timeStart)


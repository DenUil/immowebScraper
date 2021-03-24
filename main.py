from pathlib import Path
import re
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
import sqlite3
import pdfkit
import datetime


def cleanUp(strPost,  unspace=" "):
    processed_feature = "{}".format(strPost)
    processed_feature = processed_feature.replace("\n", "")
    processed_feature = processed_feature.replace("/", "_")
    processed_feature = processed_feature.replace("₂", "2")
    processed_feature = processed_feature.replace("²", "2")
    processed_feature = processed_feature.strip()
    # Substituting multiple spaces with single space
    processed_feature = re.sub(r'\s+', ' ', processed_feature, flags=re.I)
    # Converting to Lowercase
    processed_feature = processed_feature.lower()
    processed_feature = processed_feature.replace(" ", unspace)
    return processed_feature


def cleanUpURL(url, queryString):
    uu = list(urlparse(url))
    qs = parse_qs(uu[4], keep_blank_values=True)
    del (qs[queryString])
    uu[4] = urlencode(qs, doseq=True)
    return urlunparse(uu)


def initiateDatabase(con, tablename):
    cur = con.cursor()
    # Create table
    cur.execute("CREATE TABLE IF NOT EXISTS {} ('immoweb_id' 'create_date' 'modified_date')".format(tablename))
    return cur


def writeArticleToDataset(cur, con, tablename, articleAsDict):
    # check if article is already in the database
    cmd = "SELECT * FROM {} WHERE immoweb_id = {}".format(tablename,articleAsDict["immoweb_id"])
    cur.execute(cmd)  # column names

    if len(cur.fetchall()) == 0 :
        #its a new article so we will set the create date
        articleAsDict["create_date"] = datetime.datetime.now().strftime("%d/%m/%Y")
        articleAsDict["modified_date"] = ""
        # Get all column names
        cur.execute("SELECT name FROM PRAGMA_TABLE_INFO('{}')".format(tablename))  # column names
        columnsRaw = cur.fetchall()
        columns = []
        for col in columnsRaw:
            columns.append(col[0])
        #print(columns)

        # for each key in the dictionary of the article
        # check if the key already exists as a column
        for key in articleAsDict.keys():
            if not key in columns:
                # if not, create new column
                cur.execute("ALTER TABLE {} ADD COLUMN '{}' text;".format(tablename, key))
                con.commit()
                columns.append(key)
        columnsFromDict = "','".join(list(articleAsDict.keys()))
        columnsFromDict = "'{}'".format(columnsFromDict)
        dataFromDict = "','".join(list(articleAsDict.values()))
        dataFromDict = "'{}'".format(dataFromDict)
        cmd = "INSERT INTO {}({}) values ({})".format(tablename, columnsFromDict, dataFromDict)
        cur.execute(cmd)
        con.commit()
    else:
        # already exists. lets see if a parameter changed
        whereString = []
        for key in articleAsDict.keys():
            whereString.append("{} = '{}'".format(key, articleAsDict[key]))

        cmd = "SELECT * FROM {} WHERE {}".format(tablename, " AND ".join(whereString))
        cur.execute(cmd)  # column names

        if len(cur.fetchall()) == 0 :
            print("Changes found")
            articleAsDict["modified_date"] = datetime.datetime.now().strftime("%d/%m/%Y")
            # already exists. lets see if a parameter changed
            whereString = []
            for key in articleAsDict.keys():
                whereString.append("{} = '{}'".format(key, articleAsDict[key]))
            data = " , ".join(whereString)
            where = "immoweb_id = {}".format(articleAsDict["immoweb_id"])
            cmd = "UPDATE {} SET {} WHERE {}".format(tablename, data, where)
            cur.execute(cmd)  # column names
            con.commit()
        else:
            print("No changes found")

def getLinksToArticlesToScrape(urlpage):
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)
    driver.get(urlpage)
    results = driver.find_elements_by_xpath("//*[@class='search-results__pagination']")
    pages = []
    for res in results:
        answer = [e for e in re.split("[^0-9]", res.text) if e != '']
        pages.append(max(map(int, answer)))

    numberOfPages = max(pages)
    print("number Of pages: {}".format(numberOfPages))
    results = driver.find_elements_by_xpath("//*[@class='card card--result card--xl']")
    results.extend(driver.find_elements_by_xpath("//*[@class='card card--result card--large']"))
    print('Number of articles found', len(results))

    URLsToProces = []

    for article in results:
        URLsToProces.append(article.find_element_by_css_selector('a').get_attribute('href'))

    for pageNumber in range(2, numberOfPages + 1):
        print("processing page : {}".format(pageNumber))
        urlpage = 'https://www.immoweb.be/en/search/apartment/for-rent/hasselt/district?countries=BE&page={}'.format(
            pageNumber)
        driver.get(urlpage)
        results = driver.find_elements_by_xpath("//*[@class='card card--result card--xl']")
        results.extend(driver.find_elements_by_xpath("//*[@class='card card--result card--large']"))
        results.extend(driver.find_elements_by_xpath("//*[@class='card card--result card--medium']"))
        print('Number of articles found', len(results))
        for article in results:
            URLsToProces.append(article.find_element_by_css_selector('a').get_attribute('href'))

    for index, link in enumerate(URLsToProces):
        print("{} : {}".format(index, link))

    print("Size Of Link Dataset : {}".format(len(URLsToProces)))
    driver.quit()
    return URLsToProces


def fetchArticle(url, cur, con, tablename):
    rowForDatabase = {}
    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)
    driver.get(url)

    #IMMOWEB ID
    result = driver.find_elements_by_xpath("//div[@class='classified__information--immoweb-code']")
    rowForDatabase["immoweb_id"] = result[0].text.replace("Immoweb code : ","")

    #ADRES
    result = driver.find_elements_by_xpath("//div[@class='classified__information--address']")
    rowForDatabase["adres"] = result[0].text

    #result = driver.find_elements_by_xpath("//div[@id='classified-description-content-text']")
    #rowForDatabase["description"] = result[0].text

    #TABLES GENERAL, INTERIOR, EXTERIOR, FACILITIES, ENERGY, TOWN PLANNING, Financial
    for row in driver.find_elements_by_class_name('classified-table__row'):
        if len(row.find_elements_by_xpath(".//th[@class='classified-table__header']")) > 0:
            itemName = cleanUp(row.find_element_by_xpath(".//th[@class='classified-table__header']").text, unspace="_")
            itemContent = row.find_element_by_xpath(".//td[@class='classified-table__data']")
            if len(itemContent.find_elements_by_xpath(".//span[@class='sr-only']")) > 0:
                itemContentStr = cleanUp(itemContent.text.split("\n")[0])
            else:
                itemContentStr = cleanUp(itemContent.text)
            rowForDatabase[itemName] = itemContentStr

    rowForDatabase["articl_url"] = url

    #PRINT ARTICLE URL
    printURL = ""
    for result in driver.find_elements_by_xpath("//div[@class='classified-toolkit__item']"):
        printURL_statement = result.find_elements_by_xpath(".//a[@class='button button--text button--size-small']")
        if len(printURL_statement)> 0 :
            printURL = printURL_statement[0].get_attribute('href')

    # get the print link of the fotos
    driver.quit()
    if not Path("./Documents/{}".format(rowForDatabase["immoweb_id"])).exists() :
        workpath = "./Documents/{}".format(rowForDatabase["immoweb_id"])
        Path(workpath).mkdir(parents=True, exist_ok=True)
        # download fotos and artcle
        fotoFileStr = "{}/fotos_{}.pdf".format(workpath,rowForDatabase["immoweb_id"])
        articlFileStr = "{}/article_{}.pdf".format(workpath,rowForDatabase["immoweb_id"])
        pdfkit.from_url(printURL, fotoFileStr)
        pdfkit.from_url(url, articlFileStr)
        rowForDatabase["fotos_pdf_path"] = fotoFileStr
        rowForDatabase["pdf_article_path"] = articlFileStr

    writeArticleToDataset(cur, con, tablename, rowForDatabase)




if __name__ == '__main__':
    URL_dataset = getLinksToArticlesToScrape('https://www.immoweb.be/en/search/apartment/for-rent/hasselt/district?countries=BE&page=1')
    con = sqlite3.connect('immoweb_database.db')
    tableName = 'hasselt'
    cur = initiateDatabase(con, tableName)

    for url in URL_dataset:
        url_processed = cleanUpURL(url, "searchId")
        fetchArticle(url_processed, cur, con, tableName)

    con.close()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/

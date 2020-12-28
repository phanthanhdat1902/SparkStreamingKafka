import scrapy
from thpt.items import ThptItem


class CrawlerSpider(scrapy.Spider):
    name = "crawl_point"
    MaVung="02"
    start_urls = [
        'https://thanhnien.vn/giao-duc/tuyen-sinh/2020/tra-cuu-diem-thi-thpt-quoc-gia.html'
    ]

    def parse(self, response):
        SBD='02000000'
        for i in range(201,300):
            SBD=SBD[:len(SBD)-len(str(i))]+str(i)
            print(i)
            yield scrapy.Request("https://thanhnien.vn/ajax/diemthi.aspx?kythi=THPT&nam=2020&city=&text="+SBD+"&top=no", callback=self.crawlLyric)

    def crawlLyric(self, response):
        item = ThptItem()
        print(response.xpath("//td[@class='']/text()")[1].extract())
        item['SBD']=response.xpath("//td[@class='']/text()")[1].extract()
        item['MaVung']=item['SBD'][0:2]
        tab_2=["Toán","Ngữvăn","Vậtlí","Hóahọc","Sinhhọc"]
        tab_2=list(enumerate(tab_2))
        tab_3=["Lịchsử","Địalí","GDCD","Ngoạingữ","N1"]
        tab_3=list(enumerate(tab_3))
        #set default NgoaiNgu
        item['N2'] = -1
        item['N3'] = -1
        item['N4'] = -1
        item['N5'] = -1
        item['N6'] = -1
        arrTr = response.xpath("//td[@class='mobile-tab-content mobile-tab-2']")
        for i in tab_2:
        	try:
        		item[i[1]]=arrTr[i[0]].xpath("text()")[0].extract()
        	except:
        		item[i[1]]=-1
        arrTr = response.xpath("//td[@class='mobile-tab-content mobile-tab-2']")
        for i in tab_3:
        	try:
        		item[i[1]]=arrTr[i[0]].xpath("text()")[0].extract()
        	except:
        		item[i[1]]=-1
        item['N1'] = item["Ngoạingữ"]
        yield item
        # print(response.xpath('//table[@class="table thpt-mobile hidden-md hidden-sm hidden-lg"]/tr')[0].xpath('td/text()').extract())

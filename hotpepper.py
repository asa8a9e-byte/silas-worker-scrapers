"""
Hotpepper スクレイパー (SaaS Worker版)
ホットペッパービューティーの美容室情報を収集（requests + BeautifulSoup）
"""
import re
import time
from typing import Callable, List, Optional

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ================== ホットペッパービューティー エリア定義 ==================
# 3階層構造: 地方(svc) → 都市(mac) → 細分エリア(sac)
HOTPEPPER_ESTHE_AREAS = {
    "svcSD": {
        "name": "北海道",
        "cities": {
            "macDA": {"name": "札幌", "areas": {"sacX138": "札幌駅周辺", "sacX389": "大通周辺", "sacX139": "円山周辺", "sacX140": "市電沿線", "sacX141": "北区", "sacX142": "東区", "sacX143": "白石区", "sacX144": "豊平区・南区", "sacX145": "西区・手稲区", "sacX146": "厚別区・清田区"}},
            "macDB": {"name": "旭川", "areas": {"sacX214": "中心部(宮前～10条通)", "sacX215": "永山・末広・春光・大町周辺", "sacX216": "豊岡・東光・南周辺", "sacX218": "神楽・神居・緑町・旭神周辺", "sacX613": "旭川周辺(富良野・名寄)"}},
            "macDD": {"name": "函館", "areas": {"sacX408": "函館市", "sacX594": "その他函館周辺"}},
            "macDC": {"name": "道東・道北", "areas": {"sacX412": "帯広", "sacX409": "釧路", "sacX492": "北見"}},
            "macDE": {"name": "道央・道南", "areas": {"sacX411": "苫小牧", "sacX618": "恵庭・千歳・北広島", "sacX410": "江別・岩見沢・滝川", "sacX491": "小樽", "sacX490": "室蘭"}},
        }
    },
    "svcSE": {
        "name": "東北",
        "cities": {
            "macEA": {"name": "仙台・宮城", "areas": {"sacX147": "仙台駅周辺", "sacX546": "一番町・国分町", "sacX148": "泉・富谷・大和", "sacX150": "長町・仙台南", "sacX149": "宮城野区・若林区", "sacX526": "名取・岩沼・塩竈・石巻"}},
            "macEC": {"name": "岩手・盛岡", "areas": {"sacX346": "盛岡駅前", "sacX544": "中央・大通", "sacX347": "その他盛岡市内", "sacX525": "北上・一関・奥州", "sacX524": "その他岩手"}},
            "macED": {"name": "山形", "areas": {"sacX413": "山形市", "sacX414": "米沢・南陽", "sacX493": "天童・東根・村山", "sacX415": "鶴岡・酒田", "sacX494": "新庄・寒河江"}},
            "macEF": {"name": "青森", "areas": {"sacX523": "青森市", "sacX538": "弘前", "sacX420": "八戸", "sacX419": "五所川原・つがる", "sacX495": "むつ・十和田・三沢", "sacX421": "その他青森"}},
            "macEG": {"name": "秋田", "areas": {"sacX416": "秋田市", "sacX417": "横手・湯沢", "sacX418": "大館・能代", "sacX496": "由利本荘・大仙"}},
            "macEH": {"name": "福島", "areas": {"sacX227": "福島市", "sacX527": "会津若松", "sacX528": "いわき", "sacX529": "白河・須賀川"}},
            "macEI": {"name": "郡山", "areas": {"sacX220": "郡山駅前", "sacX221": "郡山市西部", "sacX222": "郡山市東部", "sacX226": "二本松・本宮"}},
        }
    },
    "svcSA": {
        "name": "関東",
        "cities": {
            "macAA": {"name": "新宿・高田馬場・代々木", "areas": {"sacX001": "東口・新宿３丁目・新宿御苑", "sacX565": "西口・南口・代々木", "sacX003": "高田馬場・新大久保"}},
            "macAB": {"name": "池袋・目白", "areas": {"sacX004": "東口・サンシャイン方面", "sacX005": "西口・北口・目白・メトロポリタン方面"}},
            "macAC": {"name": "恵比寿・広尾・六本木・麻布・赤坂", "areas": {"sacX008": "恵比寿・広尾", "sacX011": "六本木・麻布・赤坂"}},
            "macAD": {"name": "渋谷", "areas": {"sacX007": "センター街・神南・公園通り・道玄坂・神泉", "sacX111": "宮益坂・明治通り・桜丘"}},
            "macJR": {"name": "青山・表参道・原宿", "areas": {"sacX566": "青山・外苑前", "sacX009": "表参道", "sacX010": "原宿・明治神宮前"}},
            "macAE": {"name": "代官山・中目黒・自由が丘・武蔵小杉・学大", "areas": {"sacX567": "代官山・中目黒", "sacX012": "自由が丘・学芸大学", "sacX048": "日吉・綱島・大倉山・菊名", "sacX172": "武蔵小杉・田園調布・新丸子・下丸子"}},
            "macAF": {"name": "銀座・有楽町・新橋・丸の内・日本橋", "areas": {"sacX014": "銀座", "sacX568": "有楽町・日比谷・新橋・汐留", "sacX015": "丸の内・日本橋"}},
            "macAZ": {"name": "上野・浅草・北千住・亀有", "areas": {"sacX178": "上野・神田・御徒町", "sacX179": "日暮里・町屋・熊野前・舎人", "sacX180": "北千住・亀有", "sacX181": "浅草・押上・曳舟・青砥"}},
            "macAH": {"name": "目黒・五反田・品川・大崎", "areas": {"sacX019": "目黒・五反田・白金", "sacX020": "品川・田町・大崎", "sacX385": "戸越銀座・中延・西馬込・池上・旗の台", "sacX386": "武蔵小山・大岡山"}},
            "macAI": {"name": "中野・高円寺・阿佐ヶ谷", "areas": {"sacX021": "中野・東中野・新中野", "sacX022": "高円寺・新高円寺・東高円寺", "sacX023": "阿佐ヶ谷・南阿佐ヶ谷"}},
            "macAJ": {"name": "吉祥寺・荻窪・三鷹・国分寺・久我山", "areas": {"sacX024": "吉祥寺", "sacX025": "荻窪・西荻窪", "sacX026": "三鷹", "sacX308": "武蔵境・小金井・国分寺", "sacX117": "永福町・久我山"}},
            "macAK": {"name": "八王子・立川・国立・多摩・日野", "areas": {"sacX027": "八王子", "sacX028": "立川", "sacX569": "国立", "sacX134": "稲田堤・多摩センター", "sacX029": "日野", "sacX133": "聖蹟桜ヶ丘・高幡不動・北野", "sacX309": "昭島・福生・青梅", "sacX310": "秋川・五日市"}},
            "macAS": {"name": "大泉学園・石神井・練馬・所沢・飯能", "areas": {"sacX102": "江古田・練馬", "sacX112": "石神井・大泉学園・ひばりヶ丘", "sacX103": "東久留米・所沢・小手指", "sacX104": "入間・飯能・秩父", "sacX118": "新所沢・狭山市", "sacX192": "小竹向原・平和台・光が丘"}},
            "macAU": {"name": "成増・和光市・川越・東松山", "areas": {"sacX121": "大山・東武練馬", "sacX122": "成増・和光市・朝霞台", "sacX123": "志木・鶴瀬・上福岡", "sacX124": "川越・鶴ヶ島・若葉", "sacX125": "坂戸・東松山・小川町"}},
            "macAV": {"name": "沼袋・田無・小平・東村山", "areas": {"sacX126": "沼袋・鷺ノ宮", "sacX127": "上石神井・田無", "sacX128": "小平・東村山・一橋学園", "sacX129": "東大和市・拝島"}},
            "macAW": {"name": "笹塚・仙川・調布・府中", "areas": {"sacX130": "笹塚・明大前・桜上水", "sacX131": "千歳烏山・仙川・つつじヶ丘", "sacX132": "調布・府中・分倍河原"}},
            "macAX": {"name": "下北沢・成城学園・向ヶ丘遊園・新百合ヶ丘", "areas": {"sacX169": "代々木上原・下北沢", "sacX170": "経堂・成城学園・狛江", "sacX135": "登戸・向ヶ丘遊園・新百合ヶ丘・鶴川"}},
            "macAY": {"name": "二子玉川・溝の口・たまプラーザ", "areas": {"sacX013": "池尻大橋・三軒茶屋・二子玉川", "sacX136": "溝の口・たまプラーザ・あざみ野", "sacX137": "青葉台・長津田・つきみ野"}},
            "macAT": {"name": "町田・相模大野・海老名・本厚木・橋本", "areas": {"sacX113": "町田・玉川学園前・成瀬", "sacX114": "橋本・相模原・淵野辺・古淵", "sacX115": "相模大野～中央林間", "sacX116": "本厚木・海老名～小田急相模原", "sacX119": "伊勢原・秦野・渋沢", "sacX377": "大和・南林間・さがみ野"}},
            "macAP": {"name": "横浜・関内・元町・上大岡・白楽", "areas": {"sacX042": "横浜駅周辺", "sacX043": "関内・桜木町・みなとみらい", "sacX044": "元町・石川町", "sacX046": "上大岡・弘明寺", "sacX390": "反町・東白楽・白楽・妙蓮寺"}},
            "macAQ": {"name": "大井・大森・蒲田・川崎・鶴見", "areas": {"sacX577": "大井町", "sacX168": "大森・蒲田", "sacX049": "川崎", "sacX050": "鶴見", "sacX387": "子安・生麦"}},
            "macAR": {"name": "横須賀・小田原", "areas": {"sacX323": "横須賀・追浜・堀ノ内", "sacX324": "久里浜・浦賀・三浦", "sacX326": "小田原・鴨宮"}},
            "macAL": {"name": "大宮・浦和・川口・岩槻", "areas": {"sacX030": "大宮・与野", "sacX031": "浦和", "sacX032": "川口・蕨", "sacX378": "岩槻", "sacX379": "埼玉高速鉄道・武蔵野線"}},
            "macAM": {"name": "千葉・稲毛・幕張・鎌取・都賀", "areas": {"sacX033": "千葉", "sacX034": "西千葉・稲毛", "sacX381": "幕張・検見川", "sacX035": "蘇我・鎌取・都賀"}},
            "macAN": {"name": "船橋・津田沼・本八幡・浦安・市川", "areas": {"sacX037": "船橋・西船橋", "sacX036": "津田沼", "sacX038": "市川・本八幡", "sacX039": "浦安・行徳", "sacX380": "習志野・大久保・高根公団"}},
            "macAO": {"name": "柏・我孫子・松戸", "areas": {"sacX040": "柏・我孫子", "sacX041": "松戸・新松戸", "sacX382": "京成松戸線・増尾・六実・元山"}},
        }
    },
    "svcSH": {
        "name": "北信越",
        "cities": {
            "macHA": {"name": "新潟", "areas": {"sacX230": "駅前・万代・古町", "sacX612": "駅南", "sacX231": "鳥屋野潟周辺・女池・関屋", "sacX233": "東区・北区", "sacX605": "江南区・秋葉区", "sacX232": "西区・西蒲区・南区"}},
            "macHB": {"name": "長岡", "areas": {"sacX234": "長岡駅周辺", "sacX235": "川東", "sacX236": "川西", "sacX307": "その他"}},
            "macHE": {"name": "その他新潟県", "areas": {"sacX429": "上越", "sacX430": "三条・燕", "sacX431": "新発田・村上・五泉・阿賀野", "sacX432": "柏崎・南魚沼・十日町", "sacX497": "糸魚川"}},
            "macHC": {"name": "石川・金沢", "areas": {"sacX237": "片町・武蔵・杜の里・田上", "sacX238": "金沢駅・森本・福久・津幡", "sacX555": "県庁・内灘", "sacX556": "西金沢・野々市", "sacX240": "有松・久安・窪・四十万", "sacX433": "小松・白山・能美", "sacX500": "七尾", "sacX588": "加賀"}},
            "macHD": {"name": "長野", "areas": {"sacX348": "長野駅・若里・栗田", "sacX349": "三輪・高田・東和田・稲田", "sacX578": "青木島・稲里・篠ノ井", "sacX350": "その他"}},
            "macHH": {"name": "松本", "areas": {"sacX436": "松本", "sacX540": "北松本", "sacX541": "南松本"}},
            "macHI": {"name": "その他長野県", "areas": {"sacX437": "飯田", "sacX502": "岡谷・塩尻・安曇野", "sacX614": "上田", "sacX434": "佐久", "sacX435": "諏訪・茅野", "sacX522": "伊那", "sacX501": "千曲・須坂"}},
            "macHF": {"name": "富山", "areas": {"sacX592": "総曲輪・岩瀬・山室・新庄", "sacX593": "掛尾・布瀬・婦中・五福", "sacX439": "高岡・射水・氷見・砺波", "sacX610": "魚津・黒部"}},
            "macHG": {"name": "福井", "areas": {"sacX440": "福井", "sacX441": "鯖江・越前・坂井", "sacX504": "敦賀"}},
        }
    },
    "svcSC": {
        "name": "東海",
        "cities": {
            "macCA": {"name": "名駅・栄・金山・御器所・本山・大曽根", "areas": {"sacX096": "名駅・庄内通周辺", "sacX095": "栄・錦・泉・東桜・新栄", "sacX097": "大須・金山", "sacX098": "千種・池下・本山", "sacX099": "御器所・吹上周辺", "sacX100": "黒川・平安通・大曽根周辺"}},
            "macCM": {"name": "星ヶ丘・藤が丘・長久手", "areas": {"sacX365": "星ヶ丘・一社", "sacX571": "本郷・藤が丘・長久手"}},
            "macCH": {"name": "八事・平針・瑞穂・野並", "areas": {"sacX367": "新瑞橋・野並・徳重", "sacX366": "八事・平針・赤池"}},
            "macCE": {"name": "一宮・犬山・江南・小牧・小田井・津島", "areas": {"sacX355": "一宮・稲沢・清洲", "sacX356": "犬山・江南", "sacX357": "小牧", "sacX535": "小田井・上飯田周辺", "sacX534": "津島・弥富"}},
            "macCF": {"name": "春日井・尾張旭・守山・瀬戸", "areas": {"sacX536": "守山周辺", "sacX360": "尾張旭・瀬戸", "sacX359": "春日井・高蔵寺"}},
            "macCI": {"name": "名古屋港・高畑・鳴海・大府・豊明・知多・半田", "areas": {"sacX368": "名港線・神宮前・堀田・大高・鳴海", "sacX361": "中村公園・高畑・あおなみ線", "sacX371": "大府・豊明", "sacX369": "尾張横須賀・朝倉", "sacX370": "南加木屋・半田・武豊"}},
            "macCJ": {"name": "日進・豊田・刈谷・岡崎・安城・豊橋", "areas": {"sacX372": "日進・豊田・知立・刈谷・碧南", "sacX373": "岡崎・安城・西尾・蒲郡", "sacX374": "豊川・豊橋・田原"}},
            "macCB": {"name": "岐阜", "areas": {"sacX250": "岐阜駅周辺・市橋・鏡島・競輪場", "sacX251": "長良・正木・則武・島", "sacX252": "県庁・茜部・柳津・岐南周辺", "sacX254": "穂積・北方"}},
            "macCL": {"name": "各務原・大垣・関・多治見", "areas": {"sacX537": "多治見・土岐・中津川", "sacX358": "各務原・鵜沼", "sacX354": "大垣", "sacX442": "関・可児・美濃加茂・郡上", "sacX487": "高山"}},
            "macCC": {"name": "静岡・藤枝・焼津・島田", "areas": {"sacX242": "静岡駅周辺", "sacX581": "東静岡駅・草薙駅周辺", "sacX243": "葵区郊外", "sacX582": "駿河区郊外", "sacX244": "清水", "sacX444": "藤枝・島田・牧之原", "sacX604": "焼津"}},
            "macCD": {"name": "浜松・磐田・掛川・袋井", "areas": {"sacX246": "浜松街中エリア", "sacX391": "浜松駅南～南エリア", "sacX247": "遠州鉄道沿線・宮竹～東エリア", "sacX248": "高丘・初生・染地台～北エリア", "sacX249": "鴨江・佐鳴台・入野～西エリア", "sacX406": "磐田・掛川・袋井・菊川"}},
            "macCK": {"name": "その他静岡県", "areas": {"sacX405": "沼津", "sacX445": "富士", "sacX603": "富士宮", "sacX488": "三島・長泉町・裾野", "sacX489": "御殿場・伊東・伊豆"}},
            "macCG": {"name": "桑名・四日市・津・鈴鹿・伊勢", "areas": {"sacX362": "桑名・四日市", "sacX363": "鈴鹿・津", "sacX364": "松阪・伊勢・志摩", "sacX443": "名張・伊賀"}},
        }
    },
    "svcSB": {
        "name": "関西",
        "cities": {
            "macBA": {"name": "梅田・京橋・福島・本町", "areas": {"sacX055": "梅田・西梅田", "sacX056": "芝田・茶屋町・中崎町", "sacX057": "福島・野田", "sacX058": "天神橋筋", "sacX059": "京橋・都島", "sacX060": "北浜・肥後橋・本町"}},
            "macBB": {"name": "心斎橋・難波・天王寺", "areas": {"sacX061": "心斎橋", "sacX106": "西心斎橋・アメ村", "sacX062": "南船場", "sacX063": "堀江・新町", "sacX064": "難波", "sacX065": "天王寺・あべの・寺田町", "sacX542": "谷町・上本町・玉造周辺"}},
            "macBC": {"name": "江坂・千里中央・十三・豊中・池田・箕面・新大阪・吹田", "areas": {"sacX194": "池田・石橋・箕面", "sacX068": "江坂", "sacX069": "新大阪・東三国・西中島", "sacX070": "阪急十三・三国・淡路", "sacX071": "上新庄・吹田・千里丘", "sacX193": "庄内・服部・岡町・豊中", "sacX328": "緑地公園・千里中央・豊津・山田"}},
            "macBD": {"name": "茨木・高槻", "areas": {"sacX072": "高槻", "sacX073": "茨木", "sacX074": "富田・総持寺・南茨木周辺"}},
            "macBH": {"name": "門真・枚方・寝屋川・関目・守口・蒲生・鶴見", "areas": {"sacX198": "関目・守口市・門真市", "sacX199": "寝屋川市・香里園", "sacX200": "枚方市・樟葉・河内磐船・長尾", "sacX393": "蒲生・鶴見・門真南"}},
            "macBI": {"name": "鴫野・住道・四条畷・緑橋・石切・布施・花園", "areas": {"sacX205": "鴫野・放出・住道・四条畷", "sacX202": "緑橋・長田・新石切", "sacX201": "布施・河内花園・瓢箪山"}},
            "macBT": {"name": "昭和町・大正・住吉・住之江", "areas": {"sacX066": "昭和町・西田辺・帝塚山・あびこ", "sacX327": "西九条・弁天町・住之江・南港"}},
            "macBN": {"name": "平野・八尾・松原・古市・藤井寺・富田林", "areas": {"sacX329": "近鉄八尾・河内山本・堅下", "sacX330": "平野・加美・八尾・柏原", "sacX331": "駒川中野・喜連瓜破・八尾南", "sacX332": "針中野・河内天美・松原", "sacX333": "藤井寺・古市・富田林"}},
            "macBE": {"name": "堺・なかもず・深井・狭山・河内長野・鳳", "areas": {"sacX075": "堺・堺東・堺市・三国ヶ丘", "sacX076": "なかもず・深井", "sacX077": "初芝・北野田・狭山・金剛・河内長野", "sacX078": "鳳・津久野・上野芝・百舌鳥", "sacX079": "泉ヶ丘・和泉中央", "sacX394": "新金岡・北花田"}},
            "macBO": {"name": "高石・府中・岸和田・泉佐野", "areas": {"sacX337": "岸和田・貝塚・熊取", "sacX395": "高石・泉大津・和泉府中", "sacX336": "泉佐野・和泉砂川・阪南"}},
            "macBF": {"name": "京都", "areas": {"sacX081": "河原町周辺", "sacX082": "四条烏丸・御池", "sacX084": "五条～京都駅周辺・下京区・南区", "sacX396": "京阪三条周辺～岡崎", "sacX086": "東山区・祇園", "sacX107": "大宮～西院・二条・円町", "sacX083": "丸太町・今出川・北野白梅町・衣笠", "sacX397": "北大路～北山・鞍馬口駅周辺", "sacX398": "高野～修学院・松ヶ埼・上賀茂", "sacX085": "花園・太秦・嵐山・亀岡", "sacX108": "西京極・桂"}},
            "macBS": {"name": "長岡京・伏見・山科・京田辺・宇治・木津", "areas": {"sacX399": "山科区", "sacX339": "向日町・長岡京", "sacX087": "伏見桃山・深草・竹田・淀・八幡", "sacX207": "松井山手・京田辺・木津", "sacX404": "六地蔵周辺・宇治・小倉・大久保"}},
            "macBV": {"name": "舞鶴・福知山・京丹後", "areas": {"sacX446": "舞鶴・福知山", "sacX583": "その他"}},
            "macBG": {"name": "三宮・元町・神戸・兵庫・灘・東灘", "areas": {"sacX088": "三宮周辺", "sacX089": "元町周辺", "sacX543": "灘", "sacX090": "東灘", "sacX091": "神戸・兵庫"}},
            "macBL": {"name": "西宮・伊丹・芦屋・尼崎", "areas": {"sacX208": "阪急線（夙川・西宮北口・甲東園）", "sacX209": "阪急線（武庫之荘・塚口）・伊丹", "sacX210": "JR線（芦屋・西宮・甲子園口）", "sacX211": "JR線（立花・尼崎・塚本）", "sacX212": "阪神線（芦屋・西宮・鳴尾）", "sacX213": "阪神線（尼崎・千鳥橋）"}},
            "macBK": {"name": "川西・宝塚・三田・豊岡", "areas": {"sacX195": "川西・多田・畦野", "sacX196": "宝塚・中山・逆瀬川", "sacX197": "三田・ウッディタウン", "sacX447": "豊岡・丹波・篠山"}},
            "macBU": {"name": "三木・北区・西区・長田・明石・垂水", "areas": {"sacX092": "長田・須磨・垂水", "sacX093": "明石・西明石・二見", "sacX343": "地下鉄西神線・西区", "sacX344": "鈴蘭台・岡場・三木"}},
            "macBM": {"name": "姫路・加古川", "areas": {"sacX255": "姫路駅北側周辺", "sacX258": "姫路駅南側・飾磨・英賀保", "sacX256": "京口・野里・辻井・今宿方面", "sacX261": "広畑・網干・青山・太子・たつの方面", "sacX263": "加古川・東加古川", "sacX260": "御着・白浜・高砂・宝殿"}},
            "macBJ": {"name": "奈良", "areas": {"sacX203": "生駒・学園前・登美が丘", "sacX204": "西大寺・新大宮・奈良・高の原", "sacX334": "高田市・橿原神宮前・御所", "sacX335": "王寺・郡山・田原本・天理", "sacX353": "香芝・高田・八木・桜井"}},
            "macBP": {"name": "滋賀", "areas": {"sacX340": "大津・草津・守山・甲賀", "sacX341": "近江八幡・彦根・長浜", "sacX342": "大津京・堅田・安曇川"}},
            "macBR": {"name": "和歌山", "areas": {"sacX338": "和歌山", "sacX611": "岩出・紀の川・海南", "sacX486": "田辺"}},
        }
    },
    "svcSF": {
        "name": "中国",
        "cities": {
            "macFA": {"name": "広島", "areas": {"sacX151": "袋町・中町・三川町・並木通り", "sacX152": "立町・本通・紙屋町・大手町", "sacX482": "八丁堀・幟町・胡町", "sacX584": "小町・千田町・宝町", "sacX157": "広島駅周辺・牛田", "sacX154": "段原・東雲・皆実町・宇品", "sacX606": "府中・海田・安芸区", "sacX153": "横川・十日市・舟入", "sacX155": "西広島・井口・五日市", "sacX596": "廿日市", "sacX156": "安佐北区・三次", "sacX607": "安佐南区", "sacX505": "東広島・西条", "sacX449": "呉・広"}},
            "macFB": {"name": "福山・尾道", "areas": {"sacX271": "福山駅前・三吉周辺", "sacX553": "曙・新涯周辺", "sacX272": "深津・蔵王・春日・神辺周辺", "sacX450": "尾道周辺", "sacX602": "三原周辺"}},
            "macFC": {"name": "岡山・倉敷", "areas": {"sacX265": "岡山駅東口・西口", "sacX266": "表町・中山下・丸の内", "sacX564": "北長瀬・問屋町・下中野", "sacX267": "大元・青江・岡南方面", "sacX268": "津島・岡北～東岡山方面", "sacX269": "倉敷エリア", "sacX270": "中庄・庭瀬", "sacX448": "津山・玉野・笠岡・その他"}},
            "macFE": {"name": "山口", "areas": {"sacX451": "宇部・山陽小野田・美祢", "sacX506": "下関・長門", "sacX453": "山口・萩", "sacX508": "防府", "sacX608": "周南・下松", "sacX452": "岩国"}},
            "macFF": {"name": "鳥取", "areas": {"sacX454": "鳥取", "sacX455": "米子", "sacX509": "倉吉"}},
            "macFG": {"name": "島根", "areas": {"sacX457": "松江", "sacX456": "出雲", "sacX510": "益田・浜田"}},
        }
    },
    "svcSI": {
        "name": "四国",
        "cities": {
            "macIA": {"name": "高松・香川", "areas": {"sacX273": "高松市中心部", "sacX274": "高松市郊外", "sacX461": "丸亀・坂出・宇多津・善通寺・多度津", "sacX585": "三豊・観音寺・その他"}},
            "macIB": {"name": "徳島", "areas": {"sacX275": "徳島市内", "sacX276": "北島・藍住・松茂・鳴門", "sacX511": "阿南", "sacX554": "小松島・鴨島・その他徳島"}},
            "macIC": {"name": "松山・愛媛", "areas": {"sacX277": "市駅・大街道・勝山周辺", "sacX278": "清水町・山越・鴨川周辺", "sacX579": "道後・枝松・久米周辺", "sacX580": "JR松山駅・空港通り・余戸周辺", "sacX279": "朝生田・石井・古川周辺", "sacX280": "伊予・東温・松前・砥部等", "sacX459": "今治", "sacX512": "新居浜・西条・四国中央", "sacX460": "宇和島・大洲"}},
            "macID": {"name": "高知", "areas": {"sacX281": "高知市", "sacX282": "高知市周辺"}},
        }
    },
    "svcSG": {
        "name": "九州・沖縄",
        "cities": {
            "macGA": {"name": "福岡", "areas": {"sacX158": "天神・大名・今泉・赤坂・警固", "sacX160": "博多・祇園・住吉・春吉・中洲", "sacX162": "薬院・平尾・高宮", "sacX165": "大濠・西新・藤崎・姪浜", "sacX352": "吉塚・箱崎・千早・香椎", "sacX548": "福岡空港・糟屋・志免", "sacX550": "九産大・新宮・古賀・福津", "sacX167": "桜坂・六本松・別府・荒江", "sacX552": "七隈・野芥・次郎丸・橋本", "sacX551": "九大学研都市・筑前前原", "sacX163": "大橋周辺", "sacX164": "井尻・雑餉隈・春日原・大野城", "sacX479": "筑紫野・太宰府・小郡・朝倉", "sacX477": "久留米", "sacX513": "大牟田・柳川・筑後・八女", "sacX514": "宗像・遠賀", "sacX478": "飯塚・田川・嘉麻"}},
            "macGB": {"name": "北九州", "areas": {"sacX283": "小倉駅周辺", "sacX284": "小倉北・門司・戸畑", "sacX597": "小倉南・行橋・苅田", "sacX285": "黒崎・八幡東・八幡西", "sacX598": "折尾・若松・中間・直方"}},
            "macGI": {"name": "佐賀", "areas": {"sacX469": "佐賀・鳥栖・神埼", "sacX470": "唐津・伊万里・武雄"}},
            "macGC": {"name": "長崎", "areas": {"sacX286": "浜町・長崎駅周辺", "sacX288": "浜口・浦上・住吉周辺・長崎市内", "sacX557": "長崎市近郊・時津町・長与町", "sacX475": "諫早", "sacX587": "大村", "sacX476": "佐世保", "sacX515": "島原・南島原"}},
            "macGE": {"name": "熊本", "areas": {"sacX294": "上通り・上乃裏・並木坂", "sacX295": "下通り・新市街周辺・桜町", "sacX297": "水前寺・大江・子飼・新屋敷", "sacX296": "健軍・益城・戸島・嘉島・城南", "sacX599": "帯山・長嶺・月出", "sacX298": "南熊本・平成・熊本駅周辺", "sacX533": "出水・田迎・近見・川尻", "sacX299": "光の森・合志・大津・植木", "sacX467": "八代・宇土・宇城・天草", "sacX468": "荒尾・玉名・山鹿・菊池", "sacX351": "その他熊本"}},
            "macGD": {"name": "大分", "areas": {"sacX289": "府内・中央・大手町", "sacX290": "賀来・わさだ・南大分", "sacX291": "下郡・津留・萩原・高城", "sacX292": "明野・森町・鶴崎・大在", "sacX293": "大道・春日・西大分", "sacX473": "別府", "sacX474": "中津・宇佐", "sacX516": "佐伯", "sacX517": "日田"}},
            "macGF": {"name": "宮崎", "areas": {"sacX300": "宮崎市中心部", "sacX595": "吉村・一ッ葉・佐土原", "sacX301": "神宮・霧島・花ヶ島", "sacX545": "南宮崎・大塚・清武", "sacX466": "都城・日南", "sacX465": "延岡", "sacX521": "日向"}},
            "macGG": {"name": "鹿児島", "areas": {"sacX302": "天文館・鹿児島駅", "sacX303": "中央駅・城西・高麗・甲南", "sacX304": "荒田・騎射場・郡元周辺", "sacX305": "宇宿・紫原・谷山周辺", "sacX600": "伊敷・吉野", "sacX589": "姶良・加治木・蒲生", "sacX471": "霧島・国分・隼人", "sacX601": "鹿屋・大隅", "sacX472": "薩摩川内・出水・日置", "sacX518": "奄美", "sacX306": "その他鹿児島"}},
            "macGJ": {"name": "沖縄", "areas": {"sacX462": "那覇", "sacX539": "豊見城・南風原・与那原", "sacX558": "糸満・八重瀬・南城", "sacX519": "浦添・宜野湾", "sacX559": "西原・北中城・中城", "sacX463": "北谷・嘉手納・読谷", "sacX560": "沖縄市・うるま", "sacX561": "恩納村・金武・宜野座", "sacX464": "名護", "sacX520": "石垣・宮古"}},
        }
    },
}


def _extract_by_label(soup: BeautifulSoup, label: str) -> str:
    for th in soup.select("table tr th"):
        if label in th.get_text(strip=True):
            td = th.find_next("td")
            if td:
                return td.get_text(strip=True)
    for dt in soup.find_all("dt"):
        if label in dt.get_text(strip=True):
            dd = dt.find_next_sibling("dd")
            if dd:
                return dd.get_text(strip=True)
    return ""


class HotpepperEstheScraper:
    """ホットペッパービューティー美容室リスト抽出（3階層: 地方→都市→細分エリア）"""

    def __init__(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        result_callback: Optional[Callable[[dict], None]] = None,
        is_running_check: Optional[Callable[[], bool]] = None,
    ):
        self.progress_callback = progress_callback
        self.result_callback = result_callback
        self.is_running_check = is_running_check or (lambda: True)
        self.result_count = 0

    @staticmethod
    def _collect_salon_ids(html: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        ordered: List[str] = []
        seen: set[str] = set()
        for a in soup.select('a[href*="/slnH"]'):
            href = a.get("href") or ""
            m = re.search(r"(slnH\d+)", href)
            if not m:
                continue
            sid = m.group(1)
            if sid not in seen:
                seen.add(sid)
                ordered.append(sid)
        return ordered

    @staticmethod
    def _phone_from_text(text: str) -> str:
        m = re.search(r"(\d{2,4}[-‐ー]\d{2,4}[-‐ー]\d{3,4})", text)
        return m.group(1) if m else ""

    @staticmethod
    def _td_phone_ok(text: str) -> bool:
        t = text.strip()
        if len(re.sub(r"\D", "", t)) < 10:
            return False
        return bool(re.match(r"^[\d\-‐ー]+$", t))

    def _extract_phone(self, soup: BeautifulSoup, detail_url: str) -> str:
        tel_a = soup.select_one("a[href^='tel:']")
        if tel_a:
            href = (tel_a.get("href") or "").strip()
            low = href.lower()
            if low.startswith("tel:"):
                phone = href[4:].strip()
                if phone:
                    return phone
        tel_url = detail_url.rstrip("/") + "/tel/"
        try:
            r = requests.get(tel_url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                return self._phone_from_text(soup.get_text())
            tsoup = BeautifulSoup(r.text, "html.parser")
            td = tsoup.find("td", class_=re.compile(r"fs16|b"))
            if td:
                cand = td.get_text(strip=True)
                if self._td_phone_ok(cand):
                    return cand
            return self._phone_from_text(tsoup.get_text()) or self._phone_from_text(soup.get_text())
        except Exception:
            return self._phone_from_text(soup.get_text())

    def run(self, filters: dict) -> int:
        """ホットペッパービューティー美容室リスト抽出（requests + BeautifulSoup）"""
        self.result_count = 0
        areas = filters.get("areas") or []
        if not areas:
            return 0

        count = 0
        max_pages = 100

        for raw in areas:
            if not self.is_running_check():
                break
            try:
                svc_code, mac_code, sac_code, area_name = raw
            except (TypeError, ValueError):
                print(f"[HotpepperEsthe] エリア指定の形式が不正です: {raw!r}")
                continue

            print(f"[HotpepperEsthe] [SCAN] {area_name}")

            base_url = f"https://beauty.hotpepper.jp/{svc_code}/{mac_code}/salon/{sac_code}/"

            for page in range(1, max_pages + 1):
                if not self.is_running_check():
                    break

                list_url = base_url if page == 1 else f"{base_url}PN{page}.html"

                try:
                    r = requests.get(list_url, headers=HEADERS, timeout=30)
                except Exception as e:
                    print(f"[HotpepperEsthe]   リスト取得エラー page={page}: {e}")
                    break

                if r.status_code != 200:
                    print(f"[HotpepperEsthe]   ページ{page}: HTTP {r.status_code}、エリア終了")
                    break

                salon_ids = self._collect_salon_ids(r.text)
                print(f"[HotpepperEsthe]   ページ{page}: {len(salon_ids)}件")

                if not salon_ids:
                    print(f"[HotpepperEsthe]   {area_name} ページ{page}: 店舗なし、次のエリアへ")
                    break

                for salon_id in salon_ids:
                    if not self.is_running_check():
                        break

                    detail_url = f"https://beauty.hotpepper.jp/{salon_id}/"

                    try:
                        dr = requests.get(detail_url, headers=HEADERS, timeout=30)
                    except Exception as e:
                        print(f"[HotpepperEsthe]   詳細取得エラー {salon_id}: {e}")
                        time.sleep(1)
                        continue

                    if dr.status_code != 200:
                        print(f"[HotpepperEsthe]   詳細 {salon_id}: HTTP {dr.status_code}")
                        time.sleep(1)
                        continue

                    dsoup = BeautifulSoup(dr.text, "html.parser")
                    h1 = dsoup.find("h1")
                    name = h1.get_text(strip=True) if h1 else ""

                    address = _extract_by_label(dsoup, "住所")
                    hours = _extract_by_label(dsoup, "営業時間")
                    holiday = _extract_by_label(dsoup, "定休日")
                    phone = self._extract_phone(dsoup, detail_url)

                    count += 1
                    self.result_count += 1
                    if self.result_callback:
                        self.result_callback(
                            {
                                "company_name": name or salon_id,
                                "address": address,
                                "phone": phone,
                                "portal_url": detail_url,
                                "source": "ホットペッパービューティー",
                                "raw_data": {
                                    "index": count,
                                    "area_name": area_name,
                                    "salon_id": salon_id,
                                    "business_hours": hours,
                                    "holiday": holiday,
                                },
                            }
                        )
                    if self.progress_callback:
                        self.progress_callback(self.result_count, 0)

                    disp = name[:20] if name else salon_id
                    print(f"[HotpepperEsthe]   ✓ {disp}")

                    time.sleep(1)

                time.sleep(1)

            time.sleep(1.5)

        return self.result_count

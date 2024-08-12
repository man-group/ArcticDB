#include <gtest/gtest.h>

#include <arcticdb/codec/third_party/fastlanes/pack.hpp>
#include <arcticdb/codec/third_party/fastlanes/unpack.hpp>
#include <arcticdb/util/timer.hpp>
#include <numeric>

namespace helper {

uint64_t rand_arr_70_b11_w64_arr[1024] =
    {403UL, 1272UL, 863UL, 2026UL, 1646UL, 1274UL, 1737UL, 1105UL, 939UL, 311UL, 292UL, 183UL, 938UL, 1939UL, 1185UL,
     1228UL, 1714UL, 1908UL, 1155UL, 219UL, 1642UL, 1067UL, 1333UL, 70UL, 306UL, 470UL, 843UL, 1328UL, 97UL, 1135UL,
     1920UL, 800UL, 502UL, 1619UL, 190UL, 1270UL, 222UL, 1367UL, 1431UL, 632UL, 182UL, 245UL, 995UL, 1964UL, 1790UL,
     1265UL, 1998UL, 1217UL, 1444UL, 1323UL, 940UL, 881UL, 650UL, 1030UL, 442UL, 621UL, 1251UL, 654UL, 574UL, 1200UL,
     174UL, 774UL, 282UL, 1746UL, 1823UL, 1513UL, 521UL, 805UL, 1063UL, 225UL, 1970UL, 590UL, 1150UL, 1748UL, 1347UL,
     1192UL, 864UL, 1212UL, 1445UL, 262UL, 1484UL, 1589UL, 1739UL, 440UL, 1219UL, 1556UL, 1771UL, 202UL, 1UL, 1311UL,
     1377UL, 695UL, 1076UL, 1575UL, 836UL, 251UL, 1766UL, 637UL, 1919UL, 1503UL, 510UL, 218UL, 94UL, 1066UL, 671UL,
     141UL, 593UL, 1987UL, 1786UL, 962UL, 298UL, 1742UL, 579UL, 1823UL, 1868UL, 1631UL, 751UL, 378UL, 1768UL, 401UL,
     1211UL, 637UL, 330UL, 395UL, 1840UL, 190UL, 336UL, 734UL, 1848UL, 399UL, 1849UL, 2019UL, 977UL, 398UL, 747UL,
     1753UL, 856UL, 981UL, 2015UL, 1263UL, 1077UL, 8UL, 1895UL, 579UL, 2041UL, 13UL, 1850UL, 352UL, 1963UL, 1761UL,
     555UL, 872UL, 825UL, 720UL, 1442UL, 1922UL, 1924UL, 816UL, 1903UL, 244UL, 745UL, 1007UL, 358UL, 25UL, 1359UL,
     1176UL, 778UL, 1381UL, 2027UL, 294UL, 1525UL, 49UL, 1351UL, 1460UL, 959UL, 802UL, 1370UL, 679UL, 302UL, 31UL,
     1907UL, 945UL, 1991UL, 357UL, 220UL, 1483UL, 952UL, 431UL, 981UL, 1407UL, 16UL, 1286UL, 725UL, 1872UL, 790UL,
     1635UL, 440UL, 245UL, 403UL, 950UL, 72UL, 807UL, 1329UL, 2003UL, 1363UL, 403UL, 355UL, 172UL, 171UL, 1509UL, 837UL,
     249UL, 1535UL, 1653UL, 1425UL, 1404UL, 597UL, 309UL, 1433UL, 1370UL, 1934UL, 1681UL, 956UL, 2009UL, 544UL, 220UL,
     408UL, 1930UL, 120UL, 1547UL, 602UL, 229UL, 1931UL, 1638UL, 441UL, 990UL, 881UL, 1752UL, 989UL, 764UL, 620UL,
     1163UL, 497UL, 425UL, 366UL, 750UL, 1753UL, 1970UL, 1805UL, 729UL, 1029UL, 1694UL, 327UL, 1194UL, 1130UL, 1557UL,
     1352UL, 1203UL, 865UL, 420UL, 487UL, 1387UL, 1751UL, 474UL, 1730UL, 1002UL, 1121UL, 1537UL, 1141UL, 1391UL, 1449UL,
     387UL, 1972UL, 596UL, 1351UL, 1462UL, 72UL, 1602UL, 777UL, 960UL, 365UL, 866UL, 1202UL, 524UL, 1972UL, 868UL,
     106UL, 2036UL, 1011UL, 1228UL, 1607UL, 1027UL, 966UL, 1992UL, 1874UL, 1521UL, 2016UL, 1959UL, 488UL, 829UL, 1358UL,
     1398UL, 56UL, 1261UL, 777UL, 1880UL, 84UL, 295UL, 378UL, 715UL, 1471UL, 745UL, 1704UL, 571UL, 627UL, 1211UL, 904UL,
     1101UL, 1708UL, 634UL, 1346UL, 1709UL, 493UL, 612UL, 1605UL, 1621UL, 1404UL, 1810UL, 533UL, 1907UL, 1502UL, 1903UL,
     1124UL, 860UL, 1199UL, 886UL, 1686UL, 463UL, 1785UL, 1308UL, 1369UL, 767UL, 1388UL, 1950UL, 788UL, 388UL, 1502UL,
     1451UL, 393UL, 1825UL, 2022UL, 384UL, 1543UL, 897UL, 552UL, 1670UL, 1033UL, 1260UL, 612UL, 58UL, 160UL, 1347UL,
     385UL, 904UL, 888UL, 389UL, 1341UL, 134UL, 766UL, 1718UL, 1946UL, 233UL, 1587UL, 1096UL, 1991UL, 1314UL, 1009UL,
     514UL, 953UL, 1952UL, 236UL, 992UL, 1164UL, 123UL, 1802UL, 717UL, 1885UL, 1933UL, 914UL, 1962UL, 1945UL, 1537UL,
     508UL, 1433UL, 924UL, 1381UL, 1019UL, 1166UL, 1505UL, 1732UL, 1513UL, 1278UL, 593UL, 352UL, 271UL, 1528UL, 1885UL,
     545UL, 668UL, 1319UL, 1942UL, 1530UL, 1617UL, 530UL, 1905UL, 109UL, 1122UL, 719UL, 1370UL, 1208UL, 1079UL, 384UL,
     1545UL, 1258UL, 39UL, 1502UL, 1622UL, 1002UL, 2034UL, 1352UL, 1573UL, 885UL, 352UL, 1797UL, 1430UL, 476UL, 752UL,
     1641UL, 138UL, 1818UL, 176UL, 730UL, 945UL, 1312UL, 288UL, 306UL, 881UL, 1586UL, 1251UL, 1075UL, 605UL, 298UL,
     713UL, 1308UL, 267UL, 272UL, 407UL, 30UL, 1810UL, 1202UL, 1364UL, 1650UL, 1165UL, 1098UL, 235UL, 133UL, 789UL,
     1639UL, 897UL, 541UL, 282UL, 1136UL, 564UL, 186UL, 1777UL, 1843UL, 230UL, 1292UL, 1171UL, 1899UL, 4UL, 750UL,
     1276UL, 1720UL, 1615UL, 529UL, 296UL, 1106UL, 1722UL, 1355UL, 781UL, 1302UL, 1616UL, 1022UL, 850UL, 1319UL, 1054UL,
     1724UL, 1833UL, 1939UL, 639UL, 2030UL, 227UL, 261UL, 1342UL, 99UL, 1673UL, 1040UL, 241UL, 1668UL, 2033UL, 154UL,
     518UL, 1291UL, 1379UL, 966UL, 711UL, 1364UL, 1484UL, 975UL, 840UL, 812UL, 499UL, 143UL, 424UL, 1703UL, 1433UL,
     1384UL, 856UL, 1117UL, 632UL, 53UL, 523UL, 1398UL, 350UL, 1887UL, 1956UL, 532UL, 1189UL, 1874UL, 451UL, 1293UL,
     1587UL, 1798UL, 1832UL, 673UL, 808UL, 1495UL, 998UL, 704UL, 1130UL, 1586UL, 1450UL, 1399UL, 1263UL, 1288UL, 1109UL,
     909UL, 1813UL, 285UL, 893UL, 300UL, 221UL, 1683UL, 1305UL, 1801UL, 38UL, 1449UL, 1627UL, 1150UL, 1564UL, 1334UL,
     7UL, 1935UL, 491UL, 1462UL, 590UL, 1448UL, 1323UL, 1856UL, 1242UL, 558UL, 1499UL, 306UL, 984UL, 1558UL, 1268UL,
     350UL, 376UL, 1539UL, 1449UL, 1052UL, 617UL, 1537UL, 21UL, 1322UL, 1582UL, 957UL, 1383UL, 791UL, 575UL, 181UL,
     1053UL, 1837UL, 607UL, 734UL, 589UL, 826UL, 128UL, 1759UL, 1425UL, 71UL, 1924UL, 607UL, 1486UL, 1269UL, 391UL,
     1073UL, 647UL, 56UL, 776UL, 552UL, 439UL, 864UL, 984UL, 1272UL, 1546UL, 1102UL, 496UL, 648UL, 1315UL, 1182UL,
     720UL, 372UL, 1559UL, 707UL, 468UL, 1885UL, 1182UL, 1649UL, 1153UL, 2038UL, 1570UL, 1954UL, 1539UL, 21UL, 76UL,
     1395UL, 250UL, 1008UL, 977UL, 1234UL, 1832UL, 1890UL, 1020UL, 591UL, 2016UL, 106UL, 702UL, 1204UL, 2003UL, 1161UL,
     357UL, 1242UL, 578UL, 1406UL, 902UL, 788UL, 630UL, 1621UL, 1708UL, 1810UL, 214UL, 132UL, 1856UL, 1702UL, 98UL,
     1121UL, 1219UL, 161UL, 262UL, 752UL, 1550UL, 573UL, 885UL, 319UL, 466UL, 97UL, 468UL, 2026UL, 1512UL, 1518UL,
     518UL, 219UL, 556UL, 1396UL, 1751UL, 603UL, 1622UL, 1453UL, 1233UL, 769UL, 241UL, 346UL, 251UL, 1248UL, 1206UL,
     981UL, 1562UL, 912UL, 1267UL, 87UL, 866UL, 1364UL, 1940UL, 1019UL, 61UL, 1662UL, 326UL, 1633UL, 1451UL, 569UL,
     973UL, 746UL, 527UL, 1227UL, 1122UL, 6UL, 773UL, 739UL, 1002UL, 2029UL, 1521UL, 1941UL, 604UL, 688UL, 129UL, 615UL,
     507UL, 537UL, 817UL, 595UL, 1637UL, 1535UL, 1856UL, 1056UL, 1317UL, 1047UL, 137UL, 402UL, 358UL, 1270UL, 787UL,
     169UL, 517UL, 767UL, 907UL, 539UL, 576UL, 1959UL, 1890UL, 1828UL, 1714UL, 250UL, 1420UL, 1013UL, 1067UL, 1012UL,
     1539UL, 966UL, 1170UL, 1336UL, 505UL, 245UL, 1266UL, 1280UL, 1739UL, 1673UL, 671UL, 1533UL, 1140UL, 500UL, 605UL,
     135UL, 516UL, 699UL, 1161UL, 160UL, 502UL, 759UL, 1414UL, 236UL, 767UL, 638UL, 807UL, 1537UL, 1683UL, 1516UL,
     1538UL, 1926UL, 1696UL, 1508UL, 1434UL, 568UL, 459UL, 1527UL, 86UL, 182UL, 1280UL, 34UL, 1703UL, 1557UL, 62UL,
     361UL, 584UL, 430UL, 1552UL, 1657UL, 1525UL, 763UL, 2033UL, 1590UL, 1930UL, 762UL, 920UL, 51UL, 1885UL, 727UL,
     2037UL, 124UL, 1323UL, 1677UL, 1583UL, 1283UL, 1443UL, 1741UL, 22UL, 1412UL, 223UL, 1912UL, 1124UL, 814UL, 822UL,
     924UL, 938UL, 685UL, 1365UL, 894UL, 366UL, 956UL, 1535UL, 776UL, 902UL, 1612UL, 1196UL, 1288UL, 1055UL, 1458UL,
     1175UL, 1755UL, 749UL, 760UL, 749UL, 79UL, 1090UL, 975UL, 641UL, 1457UL, 809UL, 1570UL, 1965UL, 754UL, 29UL, 637UL,
     1146UL, 511UL, 1747UL, 166UL, 1904UL, 562UL, 1077UL, 1047UL, 1160UL, 328UL, 1336UL, 243UL, 767UL, 739UL, 161UL,
     1673UL, 1179UL, 205UL, 1417UL, 1214UL, 81UL, 991UL, 463UL, 250UL, 1750UL, 2005UL, 597UL, 527UL, 1850UL, 1079UL,
     983UL, 134UL, 1006UL, 1197UL, 1491UL, 1937UL, 811UL, 629UL, 878UL, 428UL, 596UL, 43UL, 450UL, 1454UL, 1582UL,
     1847UL, 724UL, 210UL, 197UL, 1257UL, 1231UL, 985UL, 602UL, 210UL, 434UL, 1428UL, 46UL, 1187UL, 1420UL, 1354UL,
     257UL, 1525UL, 659UL, 1001UL, 1993UL, 970UL, 1423UL, 102UL, 326UL, 1221UL, 1345UL, 505UL, 1381UL, 765UL, 916UL,
     1499UL, 765UL, 1810UL, 1632UL, 456UL, 117UL, 113UL, 437UL, 1910UL, 1311UL, 513UL, 945UL, 716UL, 620UL, 1673UL,
     502UL, 213UL, 1008UL, 478UL, 845UL, 1487UL, 229UL, 1683UL, 355UL, 564UL, 655UL, 68UL, 1428UL, 276UL, 65UL, 1396UL,
     872UL, 1332UL, 1966UL, 1443UL, 2001UL, 642UL, 1762UL, 561UL, 1305UL, 495UL, 412UL, 1334UL, 1834UL, 154UL, 479UL,
     1297UL, 1482UL, 1047UL, 469UL, 743UL, 1819UL, 942UL, 631UL, 670UL, 2042UL, 728UL, 1279UL, 325UL, 1997UL, 1281UL,
     2018UL, 1476UL, 866UL, 1210UL, 1358UL, 851UL, 826UL, 632UL, 1573UL, 1401UL, 1891UL, 297UL, 1015UL, 1743UL,};
}

TEST(Fastlanes, PackUnpack64) {
    auto *base64 = new uint64_t[1]();
    *base64 = 0;
    auto packed64 = new uint64_t[1024]();
    auto unpacked64 = new uint64_t[1024]();
    *(base64) = 0;
    generated::pack::fallback::scalar::pack(helper::rand_arr_70_b11_w64_arr, const_cast<uint64_t*>(packed64), 11);
    generated::unpack::fallback::scalar::unpack(packed64, const_cast<uint64_t*>(unpacked64), 11);
    for(auto k = 0; k < 1024; ++k)
    {
        ASSERT_EQ(helper::rand_arr_70_b11_w64_arr[k], unpacked64[k]);
    }
}

TEST(Fastlanes, PackSeeOrdering64) {
    using namespace arcticdb;
    auto *base64 = new uint64_t[1]();
    *base64 = 0;
    auto packed64 = new uint64_t[1024]();
    auto unpacked64 = new uint64_t[1024]();
    *(base64) = 0;
    std::vector<uint64_t> vec(1024);
    std::iota(vec.begin(), vec.end(), 0);

    generated::pack::fallback::scalar::pack(vec.data(), const_cast<uint64_t*>(packed64), 64);
    generated::unpack::fallback::scalar::unpack(packed64, const_cast<uint64_t*>(unpacked64), 64);
    for(auto k = 0; k < 1024; ++k) {
        log::version().info("{}", packed64[k]);
    }
}

TEST(Fastlanes, PackSeeOrdering8) {
    using namespace arcticdb;
    auto *base64 = new uint8_t[1]();
    *base64 = 0;
    auto packed64 = new uint8_t[1024]();
    auto unpacked64 = new uint8_t[1024]();
    *(base64) = 0;
    std::vector<uint8_t> vec(1024);
    std::iota(vec.begin(), vec.end(), 0);

    generated::pack::fallback::scalar::pack(vec.data(), const_cast<uint8_t*>(packed64), 8);
    generated::unpack::fallback::scalar::unpack(packed64, const_cast<uint8_t*>(unpacked64), 8);
    for(auto k = 0; k < 1024; ++k) {
        log::version().info("{}", packed64[k]);
    }
}

TEST(Fastlanes, PackUnpackStress) {
    using namespace arcticdb;
    auto num_runs = 100;
    auto *base64 = new uint64_t[1]();
    constexpr size_t chunk_size = 1024;
    auto total_rows = chunk_size * num_runs;
    *base64 = 0;
    auto packed64 = new uint64_t[total_rows]();
    auto unpacked64 = new uint64_t[total_rows]();
    *(base64) = 0;

    interval_timer timer;
    timer.start_timer("pack");
    for(auto k = 0; k < num_runs; ++k)
    {
        generated::pack::fallback::scalar::pack(helper::rand_arr_70_b11_w64_arr, const_cast<uint64_t*>(packed64), 11);
    }
    timer.stop_timer("pack");
    timer.start_timer("unpack");
    for(auto k = 0; k < num_runs; ++k)
    {
        generated::unpack::fallback::scalar::unpack(packed64, const_cast<uint64_t*>(unpacked64), 11);
    }
    timer.stop_timer("unpack");
    log::version().info("\n{}", timer.display_all());
}
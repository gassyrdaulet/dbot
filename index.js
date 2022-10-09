import fs from "fs/promises";
import xml2js from "xml2js";
import mysql from "mysql2/promise";
import axios from "axios";

`
git add .
git commit -m "init"
git push
`;

/*************************START CONFIG***************************/
//User
const damp = 1;
const myCity = "710000000"; //Astana
const myStoreName = "HomeTechnologies";
const myStoreId = "15503068";
const tablename = "pricelist";
const availabaleStorages = [1];
//Global
const asyncIterations = 15;
const updateEveryXMinutes = 15;
const repeat = true;
// const XMLFilePath = `./${tablename}.xml`;
const XMLFilePath = `/home/apps/jmmanager/jmmanager-server/public/${tablename}.xml`;
const dataBaseConfig = {
  host: "127.0.0.1",
  user: "root",
  database: "kaspi_price_list",
  port: "3306",
  password: "",
};

//Default
const reqUrl = "https://kaspi.kz/yml/offer-view/offers/";
const reqBody = {
  cityId: myCity,
  limit: 64,
};
const reqHeaders = {
  headers: {
    Accept: "application/json, text/*",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    Connection: "keep-alive",
    "Content-Type": "application/json; charset=UTF-8",
    Referer: "https://kaspi.kz/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent":
      "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
  },
};
/**************************END CONFIG****************************/

/**************************START SCRAPING****************************/
const updatePrices = async () => {
  const startTime = new Date();
  const conn = mysql.createPool(dataBaseConfig);
  const offers = (
    await conn.query(`SELECT * FROM ${tablename} WHERE activated = "yes"`)
  )[0];

  const newOffers = [];

  /****************START GET THE LOWEST PRICE WITHOUT HEADLESS***********************/
  const getTheLowestPrice2 = async (id, minPrice, maxPrice) => {
    let price = 0;

    const { data: concur } = await axios.post(reqUrl + id, reqBody, reqHeaders);

    if (concur.offers[0]) {
      if (concur.offers[0].merchantId === myStoreId) {
        concur.offers[0].price = concur.offers[1].price;
      }
      console.log("SKU=" + id + " scrape success!");
      succeededScrapes++;
      if (concur.offers[0].price > maxPrice) {
        price = maxPrice;
      } else if (concur.offers[0].price > minPrice) {
        if (concur.offers[0].price - minPrice < damp) {
          price = minPrice;
        } else {
          price = concur.offers[0].price - damp;
        }
      } else if (concur.offers[0].price === minPrice) {
        price = concur.offers[0].price;
      } else if (concur.offers[0].price < minPrice) {
        for (let offer of concur.offers) {
          if (offer.kaspiDelivery === false) {
            if (offer.merchantId === myStoreId) {
              continue;
            }
            if (offer.price > minPrice) {
              if (offer.price - minPrice < damp) {
                price = minPrice;
              } else {
                price = offer.price - damp;
              }
            } else if (offer.price === minPrice) {
              price = minPrice;
            } else {
              price = minPrice;
            }
            break;
          }
          price = Math.floor((minPrice + maxPrice) / 2);
        }
      }
    } else {
      console.log("SKU=" + id + " scrape failed!");
      price = maxPrice;
    }
    return price;
  };
  /****************END GET THE LOWEST PRICE WITHOUT HEADLESS***********************/

  let total = offers.length;
  let succeededScrapes = 0;

  for (let i = 0; i < Math.ceil(offers.length / asyncIterations); i++) {
    const chunk = offers.slice(
      i * asyncIterations,
      i * asyncIterations + asyncIterations
    );
    await Promise.all(
      chunk.map(async (offer) => {
        const newPrice = await getTheLowestPrice2(
          offer.suk,
          offer.minprice,
          offer.maxprice
        );

        newOffers.push({
          id: offer.id,
          actualPrice: newPrice === 0 ? offer.maxprice : newPrice,
          suk2: offer.suk2,
          model: offer.model,
          availability: offer.availability,
          availability2: offer.availability2,
          availability3: offer.availability3,
          availability4: offer.availability4,
          availability5: offer.availability5,
          brand: offer.brand,
        });
      })
    );
  }

  //START Update Database
  for (let offer of newOffers) {
    if (offer.id) {
      await conn.query(
        `UPDATE ${tablename} SET actualprice = ${offer.actualPrice}, date = CURRENT_TIMESTAMP WHERE id = ${offer.id}`
      );
    }
  }
  //END Update Database
  const finishTime = new Date();
  console.log(
    `\n${finishTime.toLocaleDateString()} ${finishTime.toLocaleTimeString()}\nSucceeded ${succeededScrapes} scrapes of ${total}.\nTook time: ${
      (finishTime - startTime) / 1000 + " seconds."
    }`
  );

  updateXML(newOffers);

  await conn.end();
  if (!repeat) {
    return;
  }
  setTimeout(() => {
    updatePrices();
  }, updateEveryXMinutes * 60 * 1000);
};

try {
  updatePrices();
} catch (e) {
  console.log(e);
}
/**************************END SCRAPING****************************/

/*************************START XML EDIT***************************/
const updateXML = async (newOffers = []) => {
  let XML = 0;
  // let XML2 = 0;
  const parser = new xml2js.Parser();
  const data = await fs.readFile(XMLFilePath);
  parser.parseString(data, function (err, result) {
    XML = result;
  });
  // const data2 = await fs.readFile("./lll.xml");
  // parser.parseString(data2, function (err, result) {
  //   XML2 = result;
  // });
  XML.kaspi_catalog.company = myStoreName;
  XML.kaspi_catalog.merchantid = myStoreId;
  delete XML.kaspi_catalog.offers[0].offer;
  //Проверить надо если добавлю еще авейлибилити будет ли ошибка?
  // let iter = 0;
  const temp = [];
  for (let offer of newOffers) {
    const availability = [];
    for (let storage of availabaleStorages) {
      switch (storage) {
        case 1:
          availability.push(offer.availability);
          break;
        case 2:
          availability.push(offer.availability2);
          break;
        case 3:
          availability.push(offer.availability3);
          break;
        case 4:
          availability.push(offer.availability4);
          break;
        case 5:
          availability.push(offer.availability5);
          break;
      }
    }
    temp.push({
      $: { sku: offer.suk2 },
      model: [offer.model],
      brand: [offer.brand],
      availabilities: [
        {
          availability,
        },
      ],
      price: [offer.actualPrice + ""],
    });
    XML.kaspi_catalog.offers[0] = { offer: temp };
  }
  if (newOffers.length === 0) {
    const availability = [{ $: { storeId: "PP1", available: "no" } }];
    const temp = [];
    temp.push({
      $: { sku: "0000000" },
      model: ["all deactivated"],
      brand: ["all deactivated"],
      availabilities: [
        {
          availability,
        },
      ],
      price: [999999 + ""],
    });
    XML.kaspi_catalog.offers[0] = { offer: temp };
  }
  // console.log(XML.kaspi_catalog.offers);
  // console.log(XML2.kaspi_catalog.offers);
  const builder = new xml2js.Builder();
  const xml = builder.buildObject(XML);
  await fs.writeFile(XMLFilePath, xml);
  console.log("XML updated successfully!");
};
/**************************END XML EDIT****************************/

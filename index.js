import fs from "fs/promises";
import xml2js from "xml2js";
import mysql from "mysql2/promise";
import axios from "axios";
import axiosRetry from "axios-retry";
import { users, offers } from "./db.js";

console.log(new Date() - 1 + 4 * 24 * 60 * 1000);
//CONFIG
const production = true;
const repeat = false;
const updateEveryXMinutes = 15;
const asyncIterations = 1000;
const asyncUserIterations = 3;
const dataBaseConfig = {
  host: "127.0.0.1",
  user: "gas",
  password: "Zeveta1559!",
  database: "kaspi_price_list",
  port: "3306",
};
const dataBaseConfig1 = {
  host: "127.0.0.1",
  user: "root",
  password: "",
  database: "kaspi_price_list",
  port: "3306",
};
const original = "./xml/original.xml";
const reqUrl = "https://kaspi.kz/yml/offer-view/offers/";
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
const XMLFilePath = `./xml/`;
const XMLFilePathProduction = `/home/apps/tgbot/public/`;
const LogFilePath = `./logs/`;
const mainLogFilePath = `./mainlog.txt`;

const start = async () => {
  const startDate = new Date();
  console.log(
    `${startDate.toLocaleDateString()} ${startDate.toLocaleTimeString()} SCRAPE START`
  );
  let mainlog = `\n\n****************************************************\n${startDate.toLocaleDateString()} ${startDate.toLocaleTimeString()} SCRAPE START\n`;
  const conn = await mysql.createConnection(
    production ? dataBaseConfig : dataBaseConfig1
  );
  //getting users
  const users = (await conn.query(`SELECT * FROM users`))[0];

  //UPDATE PRICES

  const updatePrices = async ({
    city,
    tablename,
    store_name,
    store_id,
    available_storages,
    damp,
  }) => {
    let logtext = `\n\n***************************************************************************************\n${new Date()}\n`;
    const reqBody = {
      cityId: city,
      limit: 64,
    };
    axiosRetry(axios, {
      retries: 3, // number of retries
      retryDelay: (retryCount) => {
        console.log(`retry attempt: ${retryCount}`);
        return retryCount * 2000; // time interval between retries
      },
      retryCondition: () => {
        return;
      },
    });
    const startTime = new Date();
    const offers = (
      await conn.query(`SELECT * FROM ${tablename} WHERE activated = "yes"`)
    )[0];

    const newOffers = [];

    let total = offers.length;
    let succeededScrapes = 0;

    /****************START GET THE LOWEST PRICE WITHOUT HEADLESS***********************/
    const getTheLowestPrice2 = async (id, minPrice, maxPrice) => {
      let price = 0;

      const { data: concur } = await axios
        .post(reqUrl + id, reqBody, reqHeaders)
        .catch((err) => {
          if (err) {
            throw new Error(`${err}`);
          }
        });

      if (concur.offers[0]) {
        if (concur.offers[0].merchantId === store_id) {
          concur.offers[0].price = concur.offers[1].price;
        }
        logtext += "SKU=" + id + " scrape success!\n";
        // console.log("SKU=" + id + " scrape success!");
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
              if (offer.merchantId === store_id) {
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
        logtext +=
          "SKU=" +
          id +
          " scrape failed! Seems like some data is not valid...\n";
        console.log("SKU=" + id + " scrape failed!");
        price = maxPrice;
      }
      return price;
    };
    /****************END GET THE LOWEST PRICE WITHOUT HEADLESS***********************/

    for (let i = 0; i < Math.ceil(offers.length / asyncIterations); i++) {
      const chunk = offers.slice(
        i * asyncIterations,
        i * asyncIterations + asyncIterations
      );
      await Promise.all(
        chunk.map(async (offer) => {
          try {
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
          } catch (e) {
            logtext += "SKU=" + offer.id + " scrape failed! Error: " + e + "\n";
            newOffers.push({
              id: offer.id,
              actualPrice: offer.maxprice,
              suk2: offer.suk2,
              model: offer.model,
              availability: offer.availability,
              availability2: offer.availability2,
              availability3: offer.availability3,
              availability4: offer.availability4,
              availability5: offer.availability5,
              brand: offer.brand,
            });
          }
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

    const updateXML = async () => {
      let XML = 0;
      // let XML2 = 0;
      const parser = new xml2js.Parser();
      const data = await fs.readFile(original);
      parser.parseString(data, function (err, result) {
        XML = result;
      });
      XML.kaspi_catalog.company = store_name;
      XML.kaspi_catalog.merchantid = store_id;
      delete XML.kaspi_catalog.offers[0].offer;
      const temp = [];
      available_storages = available_storages.split(",");

      for (let offer of newOffers) {
        const availability = [];
        for (let storage of available_storages) {
          switch (storage) {
            case "1":
              availability.push(offer.availability);
              break;
            case "2":
              availability.push(offer.availability2);
              break;
            case "3":
              availability.push(offer.availability3);
              break;
            case "4":
              availability.push(offer.availability4);
              break;
            case "5":
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
      await fs.writeFile(
        (production ? XMLFilePathProduction : XMLFilePath) + tablename + ".xml",
        xml
      );
      logtext += "XML updated successfully!";
      await fs.writeFile(LogFilePath + tablename + ".txt", logtext);
    };
    updateXML();

    const finishTime = new Date();
    logtext += `\n${finishTime.toLocaleDateString()} ${finishTime.toLocaleTimeString()}\nSucceeded ${succeededScrapes} scrapes of ${total}.\nTook time: ${
      (finishTime - startTime) / 1000 + " seconds."
    }\n`;
  };

  //UPDATE PRICES
  for (let i = 0; i < Math.ceil(users.length / asyncUserIterations); i++) {
    const chunk = users.slice(
      i * asyncUserIterations,
      i * asyncUserIterations + asyncUserIterations
    );
    console.log(
      `CHUNK: [${chunk.map((props) => {
        return props.tablename;
      })}]`
    );
    await Promise.all(
      chunk.map(async (props) => {
        await updatePrices(props);
      })
    ).then(() => {
      const finishDate = new Date();
      console.log(
        `${finishDate.toLocaleTimeString()} CHUNK [${chunk.map((props) => {
          return props.tablename;
        })}] SCRAPE END Took time: ${(finishDate - startDate) / 1000}s`
      );
      mainlog += `\n${finishDate.toLocaleTimeString()} CHUNK [${chunk.map(
        (props) => {
          return props.tablename;
        }
      )}] SCRAPE END Took time: ${(finishDate - startDate) / 1000}s`;
    });
  }
  await conn.end();
  console.log("ALL ENDED");
  mainlog += `\nALL ENDED`;
  await fs.appendFile(mainLogFilePath, mainlog);
  if (!repeat) {
    return;
  }
  setTimeout(start, updateEveryXMinutes * 60 * 1000);
};
start();

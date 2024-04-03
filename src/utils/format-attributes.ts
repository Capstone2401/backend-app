import { QueryResult, QueryResultRow } from "pg";

interface FormattedAttributes {
  event: {
    [key: string]: string[];
  };
  user: {
    [key: string]: string[];
  };
}

function formatAttributes(result: QueryResult[]) {
  let attributeObj: FormattedAttributes = {
    event: getDistinctAttributes(result[0]),
    user: getDistinctAttributes(result[1]),
  };

  return attributeObj;
}

interface Attributes {
  [key: string]: string[];
}

interface ParsedJson {
  [key: string]: unknown;
}

function getDistinctAttributes(attributeArr: QueryResult) {
  const formattedAtrributes: Attributes = {};
  attributeArr.rows.forEach((row: QueryResultRow) => {
    let attributes: ParsedJson = {};

    try {
      attributes = JSON.parse(row.json_serialize);
    } catch (error) {
      if (error instanceof Error) {
        console.log("Error when parsing attributes as JSON", error.message);
        throw error;
      }
    }

    for (let attribute in attributes) {
      const value = attributes[attribute];
      if (!formattedAtrributes.hasOwnProperty(attribute)) {
        formattedAtrributes[attribute] = [];
      }
      if (
        typeof value === "string" &&
        !formattedAtrributes[attribute].includes(value)
      ) {
        formattedAtrributes[attribute].push(String(value));
      }
    }
  });

  return formattedAtrributes;
}

export default formatAttributes;

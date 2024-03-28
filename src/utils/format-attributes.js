function formatAttributes(result) {
  let attributeObj = {
    event: getDistinctAttributes(result[0]),
    user: getDistinctAttributes(result[1]),
  };

  return attributeObj;
}

function getDistinctAttributes(attributeArr) {
  let formattedAtrributes = {};
  attributeArr.rows.forEach((row) => {
    let attributes = JSON.parse(row.json_serialize);
    for (let attribute in attributes) {
      let value = attributes[attribute];
      if (!formattedAtrributes.hasOwnProperty(attribute)) {
        formattedAtrributes[attribute] = [];
      }
      if (!formattedAtrributes[attribute].includes(value)) {
        formattedAtrributes[attribute].push(String(value));
      }
    }
  });

  return formattedAtrributes;
}

module.exports = formatAttributes;

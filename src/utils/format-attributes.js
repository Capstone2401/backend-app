function formatAttributes(result) {
  let attributeObj = {
    event: getDistinctAttributes(result[0]),
    user: getDistinctAttributes(result[1])
  }

  return attributeObj;
}

function getDistinctAttributes(attributeArr) {
  let formattedAttributes = {};
  attributeArr.rows.forEach(row => {
    let attributes = JSON.parse(row.json_serialize);
    for (let attribute in attributes) {
      let value = attributes[attribute];
      if (!newObj.hasOwnProperty(attribute)) {
        newObj[attribute] = [];
      }
      if (!newObj[attribute].includes(value)) {
        newObj[attribute].push(String(value))
      }
    }
  })
  return formattedAttributes;
}

module.exports = formatAttributes
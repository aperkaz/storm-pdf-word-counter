// tuple related methods
function isValidTuple(tuple){
  if(
    typeof getWord(tuple) !== 'undefined'  &&
    typeof getCount(tuple) !== 'undefined'   &&
    getCount(tuple) !== 0
  ){
    return true;
  }else {
    return false
  }
}

function getPdfTitle(event){
  return event.data.split("|")[0];
}

function getWord(event){
  return event.data.split("|")[1];
}

function getCount(event){
  return Number(event.data.split("|")[2]);
}

// pdf related methods
function initializePdf(pdfTitle){
  var pdfObject = {};
  pdfObject['title'] = pdfTitle;
  pdfObject['words'] = [];
  pdfObject['counts'] = [];
  pdfObject['minCount'] = Number(0);
  return pdfObject;
}

function retrievePdfIndex(pdfTitle){
  for(var index = 0; index < pdfData.length ; index++){
    if(pdfData[index]['title'] === pdfTitle){
      return index;
    }
  }
  return -1;
}

function isWordStored(pdfIndex, word){
  return pdfData[pdfIndex]['words'].includes(word);
  }

function isNewWordValid(pdfIndex, word,count){
  if(pdfData[pdfIndex]['minCount'] < count){
    return true;
  } else {
      if(pdfData[pdfIndex]['words'].length < MAX_WORDS_PER_FILE)
        return true;
  }
  return false;
}

function addWord(pdfIndex, word, count){
  if(pdfData[pdfIndex]['words'].length === 0){
    pdfData[pdfIndex]['words'].splice(0, 0, word);
    pdfData[pdfIndex]['counts'].splice(0, 0, count);
    return;
  }

  for( var index = 0; index < pdfData[pdfIndex]['words'].length ; index++){
    if( pdfData[pdfIndex]['counts'][index] < count){
      pdfData[pdfIndex]['words'].splice(index, 0, word);
      pdfData[pdfIndex]['counts'].splice(index, 0, count);
      return;
    }
  }
  if( pdfData[pdfIndex]['words'].length < MAX_WORDS_PER_FILE){
    var index = pdfData[pdfIndex]['words'].length ;
    pdfData[pdfIndex]['words'].splice(index, 0, word);
    pdfData[pdfIndex]['counts'].splice(index, 0, count);
  }
}

function getWordIndex(pdfIndex, word){
  return pdfData[pdfIndex]['words'].indexOf(word);
}

function updateWordValues(pdfIndex, wordIndex, newCount){
  pdfData[pdfIndex]['counts'][wordIndex] = newCount;
}

function reorderWord(pdfIndex, wordIndex){

    var count = pdfData[pdfIndex]['counts'][wordIndex];
    var newIndex;

    for( var i = 0; i < wordIndex ; i++){
      if(count > pdfData[pdfIndex]['counts'][i]){
        newIndex = i;
        shiftArrPos(pdfData[pdfIndex]['words'], wordIndex, i);
        shiftArrPos(pdfData[pdfIndex]['counts'], wordIndex, i);
        break;
      }
    }

    if(newIndex > 0)
      reorderWord(pdfIndex,newIndex);

}

function shiftArrPos(arr, index1, index2){
      var temp = arr[index1];
      arr[index1] = arr[index2];
      arr[index2] = temp;
    }

function removeExtraWords(pdfIndex){
  pdfData[pdfIndex]['words'] = pdfData[pdfIndex]['words'].slice(0, MAX_WORDS_PER_FILE);
  pdfData[pdfIndex]['counts'] = pdfData[pdfIndex]['counts'].slice(0, MAX_WORDS_PER_FILE);
}

function updateMinCount(pdfIndex){
    pdfData[pdfIndex]['minCount'] = computeNewMin(pdfData[pdfIndex]);
}

var updateBookInformation = function () {
  // update tables with information
  var html = '';

  for (var index = 0 ; index < pdfData.length ; index++) {
      // create the new table per book
      if(pdfData[index]['words'].length > 0){
        var table = '<div style="margin: 50px 150px; border-radius: 10px; '+
                    'border: 2px solid #55ACEE; '+
                    'padding: 20px;">';
        table += createTableForBook(pdfData[index]);
        html += table + '</div>';
      }
  }


  document.getElementById("data").innerHTML = html;

};

function createTableForBook(bookData){
  var table =
   '<table class="table" style="text-align: center;"> '+
  '   <thead> '+
  '     <tr> '+
  '       <th class="text-center">' + bookData['title'] + '</th> '+
  '     </tr>'+
  '   </thead>'+
  '   <tbody>';

  var tableBody = '';
  for(var index = 0; index < bookData['words'].length ; index++){
      tableBody +=  '     <tr>  '+
                    '      <td style="width: 50%">' + bookData['words'][index] + '</td>   '+
                    '      <td style="width: 50%">' + bookData['counts'][index] + '</td>  '+
                    '     </tr>';
  }
  table += tableBody;

  table +=  '   </tbody> '+
            '</table>';
  return table;
}

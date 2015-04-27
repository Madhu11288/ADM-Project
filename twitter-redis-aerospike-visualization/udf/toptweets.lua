local DEBUG = true
local GD

function top(flow, top_size)
  --info("Top size:"..tostring(top_size))
  print("Top size:"..tostring(top_size))

  local function transformer(rec)
    --info("rec:"..tostring(rec))
    local touple = map()
    touple["hashTag"] = rec["hashTag"]
    touple["hashTagCount"] = rec["hashTagCount"]
    --info("touple:"..tostring(touple))
    return touple
  end

  local function movedown(theList, size, at, element)
    --info("List:"..tostring(theList)..":"..tostring(size)..":"..tostring(start))
    if at > size then
      info("You are an idiot")
      return
    end
    index = size-1
    while (index > at) do
      theList[index+1] = theList[index]
      index = index -1
    end

    theList[at] = element
  end

  local function accumulate(aggregate, nextitem)
    local aggregate_size = list.size(aggregate)
      info("Accumulator - size:"..tostring(aggregate_size).." Aggregate:"..tostring(aggregate))
      --info("Item:"..tostring(nextitem))
      index = 1
      for value in  list.iterator(aggregate) do
        --info(tostring(nextitem.hashTagCount).." > "..tostring(value.hashTagCount))
        if nextitem.hashTagCount > value.hashTagCount then
          movedown(aggregate, top_size, index, nextitem)
          break
        end
        index = index + 1
      end
    return aggregate
  end

  local function reducer( this, that )
    local merged_list = list()
    local this_index = 1
    local that_index = 1
    while this_index <= 10 do
      while that_index <= 10 do
        if this[this_index].hashTagCount >= that[that_index].hashTagCount then
          list.append(merged_list, this[this_index])
          this_index = this_index + 1
        else
          list.append(merged_list, that[that_index])
          that_index = that_index +1
        end
        if list.size(merged_list) == 10 then
          break
        end
      end
      if list.size(merged_list) == 10 then
        break
      end
    end
    --info("This:"..tostring(this).." that:"..tostring(that))
    return merged_list
  end

  return flow:map(transformer):aggregate(
          list{
              map{hashTag=10,hashTagCount=0},
              map{hashTag=9,hashTagCount=0},
              map{hashTag=8,hashTagCount=0},
              map{hashTag=7,hashTagCount=0},
              map{hashTag=6,hashTagCount=0},
              map{hashTag="five",hashTagCount=0},
              map{hashTag="four",hashTagCount=0},
              map{hashTag="three",hashTagCount=0},
              map{hashTag="two",hashTagCount=0},
              map{hashTag="one",hashTagCount=0}
          }, accumulate):reduce(reducer)

end
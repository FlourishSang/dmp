package utils

/**
  * @BelongsProject: dmp
  * @BelongsPackage: utils
  * @Author: Flourish Sang
  * @CreateTime: 2019-02-27 14:58
  * @Description: ${Description}
  */
object NBF {
  def toInt(str:String):Int={
    try{
      str.toInt
    }catch{
      case _ : Exception => 0
    }
  }
  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch{
      case _ :Exception => 0.0
    }
  }
}

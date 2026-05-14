// Databricks notebook source
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

// COMMAND ----------

object date_utils extends Serializable {
    val calendar = Calendar.getInstance()
    
    def getToday: String = {
        //val now = new Date()
        //val format = new SimpleDateFormat("yyyyMMdd")
        //val today = format.format(now)
        val today = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date(System.currentTimeMillis()))
        today
    }
    
    def getToday_1: String = {
        val today = new SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date(System.currentTimeMillis()))
        today
    }
    
    def getTodayTimeStamp: String = {
        //val now = new Date()
        //val format = new SimpleDateFormat("yyyyMMdd")
        //val today = format.format(now)
        val today = new SimpleDateFormat("yyyyMMddHHmmss").format(new java.util.Date(System.currentTimeMillis()))
        today
    }
    
    def getDateTime: String = {
        val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val today = calendar.getTime()
        date.format(today)
    }
    
    def getYesterday: String = {
        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        calendar.add(Calendar.DATE, -1)
        var yesterday = format.format(calendar.getTime())
        yesterday
    }
    
    def getCurrentTime: String = {
        val now = new Date()
        val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
        val CurrentTime = format.format(now)
        CurrentTime
    }
    
    def getNowYear(): String = {
        var now: Date = new Date()
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy")
        var year = dateFormat.format(now)
        year
    }
    
    def CurrentTimeStamp: String = {
        val timestamp: Long = System.currentTimeMillis / 1000
        return timestamp.toString()
    }
}

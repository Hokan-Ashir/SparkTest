package ru.hokan

import eu.bitwalker.useragentutils.{Browser, UserAgent}
import ru.hokan.BrowserTypes.BrowserTypes

class UserAgentParser {
  def getBrowserType(value : String): BrowserTypes = {
    val userAgent: UserAgent = UserAgent.parseUserAgentString(value)
    val name: Browser = userAgent.getBrowser.getGroup
    if (name.eq(Browser.IE)) {
      BrowserTypes.IE
    } else if (name.eq(Browser.MOZILLA)) {
      BrowserTypes.MOZILLA
    } else  {
      BrowserTypes.OTHER
    }
  }
}

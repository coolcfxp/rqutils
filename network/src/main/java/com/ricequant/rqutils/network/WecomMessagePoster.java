package com.ricequant.rqutils.network;

import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kangol
 */
public class WecomMessagePoster {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public WecomMessagePoster(WebClient client, String url) {
    // FIXME: Re-enable Wecom webhook posting when webhook URL is configured
  }

  public void post(String message) {
    logger.warn("[WECOM-ALERT] {}", message);
  }
}

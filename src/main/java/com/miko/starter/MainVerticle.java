package com.miko.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;

public class MainVerticle extends AbstractVerticle {

  private JDBCClient jdbcClient;

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new MainVerticle());
  }

  @Override
  public void start() {

    jdbcClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:mysql://localhost:3306/app_installation")
      .put("driver_class", "com.mysql.cj.jdbc.Driver")
      .put("user", "root")
      .put("password", "root"));


    processApps();
  }

  private void processApps() {

    jdbcClient.getConnection(connectionResult -> {
      if (connectionResult.succeeded()) {
        SQLConnection connection = connectionResult.result();


        connection.query("SELECT * FROM apps WHERE state = 'SCHEDULED' LIMIT 1", queryResult -> {
          if (queryResult.succeeded() && !queryResult.result().getRows().isEmpty()) {
            JsonObject app = queryResult.result().getRows().get(0);
            int appId = app.getInteger("id");


            installApp(connection, appId);
          } else {
            System.out.println("No apps to process.");
            connection.close();
          }
        });
      } else {
        System.err.println("Failed to connect to the database: " + connectionResult.cause());
      }
    });
  }

  private void installApp(SQLConnection connection, int appId) {

    updateState(connection, appId, "PICKEDUP", updateResult -> {
      if (updateResult.succeeded()) {
        System.out.println("Installing app ID: " + appId);


        boolean success = Math.random() > 0.3;

        if (success) {

          updateState(connection, appId, "COMPLETED", completeResult -> {
            if (completeResult.succeeded()) {
              System.out.println("App ID " + appId + " installed successfully.");
            }
            connection.close();
            processApps();
          });
        } else {
          handleRetryOrError(connection, appId);
        }
      } else {
        System.err.println("Failed to update app to PICKEDUP state.");
        connection.close();
      }
    });
  }

  private void handleRetryOrError(SQLConnection connection, int appId) {
    connection.query("SELECT retry_count FROM apps WHERE id = " + appId, retryQuery -> {
      if (retryQuery.succeeded()) {
        int retryCount = retryQuery.result().getResults().get(0).getInteger(0);

        if (retryCount < 3) {
          connection.update("UPDATE apps SET retry_count = retry_count + 1 WHERE id = " + appId, retryUpdate -> {
            if (retryUpdate.succeeded()) {
              System.out.println("Retrying app ID: " + appId);
              installApp(connection, appId);
            } else {
              System.err.println("Failed to increment retry count for app ID: " + appId);
            }
          });
        } else {

          updateState(connection, appId, "ERROR", errorUpdate -> {
            if (errorUpdate.succeeded()) {
              System.out.println("App ID " + appId + " failed after 3 retries. Sending notification.");
              sendEmailNotification(appId);
            }
            connection.close();
            processApps();
          });
        }
      } else {
        System.err.println("Failed to fetch retry count for app ID: " + appId);
        connection.close();
      }
    });
  }

  private void updateState(SQLConnection connection, int appId, String state, io.vertx.core.Handler<io.vertx.core.AsyncResult<Void>> handler) {

    connection.update("UPDATE apps SET state = '" + state + "' WHERE id = " + appId, updateResult -> {
      if (updateResult.succeeded()) {
        System.out.println("App ID " + appId + " state updated to " + state);
        handler.handle(io.vertx.core.Future.succeededFuture());
      } else {
        System.err.println("Failed to update state to " + state + " for app ID: " + appId);
        handler.handle(io.vertx.core.Future.failedFuture(updateResult.cause()));
      }
    });
  }

  private void sendEmailNotification(int appId) {
    System.out.println("Email notification sent for app ID: " + appId);
  }
}


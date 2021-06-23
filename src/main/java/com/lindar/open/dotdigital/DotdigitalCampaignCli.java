package com.lindar.open.dotdigital;

import com.github.bogdanovmn.cmdline.CmdLineAppBuilder;
import com.lindar.dotmailer.Dotmailer;
import com.lindar.dotmailer.vo.api.AccountInfo;
import com.lindar.dotmailer.vo.api.CampaignInfo;
import com.lindar.wellrested.vo.Result;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class DotdigitalCampaignCli {

    private static final RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                                                                                .limitRefreshPeriod(Duration.ofMinutes(1))
                                                                                .limitForPeriod(5)
                                                                                .timeoutDuration(Duration.ofHours(1))
                                                                                .build();

    // Create registry
    private static final RateLimiterRegistry rateLimiterRegistry = RateLimiterRegistry.of(rateLimiterConfig);

    // Use registry
    private static final RateLimiter rateLimiter = rateLimiterRegistry.rateLimiter("updateCampaign");

    private static final RetryConfig retryConfig = RetryConfig.custom()
                                                              .maxAttempts(5)
                                                              .intervalFunction(IntervalFunction.ofExponentialBackoff())
                                                              .build();

    // Create a RetryRegistry with a custom global configuration
    private static final RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
    private static final Retry         retry         = retryRegistry.retry("updateCampaign");

    public static void main(String[] args) {
        try {
            new CmdLineAppBuilder(args)
                    .withJarName("dotdigital-campaign-cli") // just for a help text (-h option)
                    .withDescription("Tool for updating dotdigital campaigns")
                    .withRequiredArg("username", "Dotdigital API username")
                    .withRequiredArg("password", "Dotdigital API password")
                    .withRequiredArg("find", "The text to find in the campaign")
                    .withRequiredArg("replace", "The text to replace in the campaign")
                    .withArg("campaign", "campaign id to be updated")
                    .withFlag("all-campaigns", "all campaigns to be updated")
                    .withFlag("dry-run", "no campaigns will be updated, only logged")
                    .withAtLeastOneRequiredOption("campaign", "all-campaigns")

                    .withEntryPoint(
                            DotdigitalCampaignCli::execute
                    ).build().run();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    private static void execute(CommandLine cmdLine) {
        if (cmdLine.hasOption("campaign") && cmdLine.hasOption("all-campaigns")) {
            System.out.println("Only one of 'campaign' or 'all-campaigns' options should be set");
            return;
        }

        boolean dryRun = cmdLine.hasOption("dry-run");
        String find = cmdLine.getOptionValue("find");
        String replace = cmdLine.getOptionValue("replace");

        if (dryRun) {
            System.out.println("Dry run is enabled, no campaigns will be updated");
        }

        System.out.println("Finding text '" + find + "' and replacing with '" + replace + "'");

        Dotmailer dotmailer = Dotmailer.build(cmdLine.getOptionValue("username"), cmdLine.getOptionValue("password"));
        Result<AccountInfo> accountInfoResult = dotmailer.accountInfo().get();
        if (accountInfoResult.isFailed()) {
            System.out.println("Failed to get account info with error: " + accountInfoResult.getMsg());
            System.out.println("Check the username and password are correct");
            return;
        }

        List<CampaignInfo> campaigns = new ArrayList<>();
        if (cmdLine.hasOption("campaign")) {
            String campaignIdString = cmdLine.getOptionValue("campaign");
            try {
                long campaignId = Long.parseLong(campaignIdString);
                Result<CampaignInfo> campaignResult = dotmailer.campaign().info(campaignId);
                if (campaignResult.isFailed()) {
                    System.out.println("Unable to fetch campaign with id [" + campaignIdString + "], failed with error: " + campaignResult.getMsg());
                    return;
                }
                campaigns.add(campaignResult.getData());
            } catch (NumberFormatException exception) {
                System.out.println("Unable to parse [" + campaignIdString + "] for the campaign id");
                return;
            }
        } else if (cmdLine.hasOption("all-campaigns")) {
            Result<List<CampaignInfo>> listResult = dotmailer.campaign().list();
            if (listResult.isFailed()) {
                System.out.println("Unable to fetch all campaigns, failed with error: " + listResult.getMsg());
                return;
            }

            campaigns.addAll(listResult.getData());
        }

        System.out.println("Found " + campaigns.size() + " campaigns");

        int updated = 0;
        for (CampaignInfo campaign : campaigns) {
            boolean success = false;
            try {
                success = rateLimiter.executeCallable(() -> retry.executeCallable(() -> updateCampaign(dotmailer, campaign, find, replace, dryRun)));
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            if (success) {
                updated++;
            }
        }
        if (dryRun) {
            System.out.println(updated + " with matching text, not updated");
            System.out.println("Run without dry-run to update them");
        } else {
            System.out.println(updated + " with matching text, updated");
        }
        System.out.println("Finished");
    }

    private static boolean updateCampaign(Dotmailer dotmailer, CampaignInfo campaign, String find, String replace, boolean dryRun) {
        System.out.println("Processing campaign " + campaign.getId() + ", " + campaign.getName());
        if (campaign.getHtmlContent() == null) {
            Result<CampaignInfo> campaignResult = dotmailer.campaign().info(campaign.getId());
            if (campaignResult.isFailed()) {
                System.out.println("Unable to fetch campaign with id [" + campaign.getId() + "], failed with error: " + campaignResult.getMsg());
                throw new RuntimeException("Failed updating campaign");
            }
            campaign = campaignResult.getData();
        }

        if ((campaign.getHtmlContent() == null || !campaign.getHtmlContent().contains(find))
                && (campaign.getPlainTextContent() == null || !campaign.getPlainTextContent().contains(find))) {
            return false;
        }

        if (dryRun) {
            System.out.println("- found - " + campaign.getId() + ", " + campaign.getName());
            return true;
        }

        campaign.setHtmlContent(StringUtils.replace(campaign.getHtmlContent(), find, replace));
        campaign.setPlainTextContent(StringUtils.replace(campaign.getPlainTextContent(), find, replace));
        Result<CampaignInfo> update = dotmailer.campaign().update(campaign);
        if (update.isFailed()) {
            System.out.println("Unable to update campaign with id [" + campaign.getId() + "], failed with error: " + update.getMsg());
            return false;
        }

        System.out.println("- updated - " + campaign.getId() + ", " + campaign.getName());

        return true;
    }
}

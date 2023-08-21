# Fixity

Fixity implementation for Medusa

## Installation
Clone the repository onto an EC2 instance (c6g.large)

Set up monit and crontab

## Usage
Utilize crontab to consistently fetch sqs responses from s3 , initiate batch restorations and process batch reports.

Set up Monit to constantly run fixity.

Update the CurrentId in the FixityProdMedusaDBIdTable to 0 to restart fixity.

Monitor progress via logs on the EC2 (/home/ec2-user/logs) or CloudWatch and monitor email for error/done alarm notifications.

## Development
See https://wiki.illinois.edu/wiki/display/scrs/Medusa+Fixity

## Testing
To run all tests use `rake test`

To run a specific test use `rake test TEST=test/{test_class.rb}` 

Ex `rake test TEST=test/fixity_test.rb`

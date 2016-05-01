<?php
//個人メモ：デプロイした時はmysql_connectとdatabase名を変更しておくこと

/*	MySQL tableメモ
create table `users`(
`id` int unsigned not null auto_increment primary key,
`twitter_id_str` varchar(64) not null  unique,
`screen_name` varchar(64) not null,
`name` varchar(64),
`description` text default '',
`url` text default '',
`favourites_count` int unsigned not null default 0,
`protected` tinyint(3) unsigned not null default 0,
`friends_count` int unsigned not null default 0,
`followers_count` int unsigned not null default 0,
`language` varchar(16) not null,
`verified` tinyint(3) unsigned not null  default 0,
`statuses_count` int unsigned not null default 0,
`profile_image_url` text default '',
`profile_background_image_url` text default '',
`geo_enabled` tinyint(3) unsigned  not null default 0,
`location` varchar(16),
`created` datetime not null,
`modified` datetime not null,
`disabled`  tinyint(3) unsigned not null default 0
)engine=InnoDB default charset=utf8;

create table `tweets`(
`id` int unsigned not null auto_increment primary key,
`tweet_id_str` varchar(64) not null  unique,
`user_id` int unsigned not null,
`text` text default '',
`retweet_count` int unsigned not null default 0,
`favorite_count` int unsigned not null default 0,
`source` varchar(64),
`country` varchar(64),
`full_name` varchar(64),
`created` datetime NOT NULL,
`modified` datetime NOT NULL,
`disabled` tinyint(3) unsigned not null default 0
)engine=InnoDB default charset=utf8;
*/

require_once('../phirehose/lib/OauthPhirehose.php');

/**
 * Barebones example of using OauthPhirehose to do user streams.
 *
 * This shows how to get user streams by just passing Phirehose::METHOD_USER
 *  as the 3rd parameter to the constructor, instead of using the UserStreamPhirehose
 *  class.
 */
class MyUserConsumer extends OauthPhirehose
{
  /**
   * First response looks like this:
   *    $tweetData=array('friends'=>array(123,2334,9876));
   *
   * Each tweet of your friends looks like:
   *   [id] => 1011234124121
   *   [text] =>  (the tweet)
   *   [user] => array( the user who tweeted )
   *   [entities] => array ( urls, etc. )
   *
   * Every 30 seconds we get the keep-alive message, where $status is empty.
   *
   * When the user adds a friend we get one of these:
   *    [event] => follow
   *    [source] => Array(   my user   )
   *    [created_at] => Tue May 24 13:02:25 +0000 2011
   *    [target] => Array  (the user now being followed)
   *
   * @param string $status
   */
	public function enqueueStatus($status)
	{
    /*
     * In this simple example, we will just display to STDOUT rather than enqueue.
     * NOTE: You should NOT be processing tweets at this point in a real application, instead they
     *  should be being enqueued and processed asyncronously from the collection process.
     */
		 try {
			$mysqlConnect = mysqli_connect("host_name", "user_name", "password") or die("mysql connect failed");

			if (mysqli_select_db($mysqlConnect, "twitter_db")) {
				date_default_timezone_set('Asia/Tokyo');
				$tweetData = json_decode($status, true);
				//print_r($tweetData);

				if (is_array($tweetData) && (! array_key_exists('event', $tweetData)) && (array_key_exists('text', $tweetData))) {
					if (is_array($tweetData) && array_key_exists('retweeted_status', $tweetData)) {
						$userData = $tweetData['retweeted_status']['user'];
						$retweetData = $tweetData['retweeted_status'];
						$retweetedUserData = mysqli_fetch_assoc(mysqli_query($mysqlConnect, "SELECT id, twitter_id_str, screen_name, disabled FROM users WHERE twitter_id_str = '{$userData['id_str']}';"));	//get id in users table (this id use to record tweet)
						$existRetweet = mysqli_fetch_array(mysqli_query($mysqlConnect, "SELECT count(*) AS sum FROM tweets WHERE tweet_id_str = '{$retweetData['id_str']}';"), MYSQLI_ASSOC);

						//mysqlにはfalse -> 0，true -> 1を入れたい
						$userData['protected'] = ($userData['protected'] == false) ? 0 : 1;
						$userData['verified'] = ($userData['verified'] == false) ? 0 : 1;
						$userData['geo_enabled'] = ($userData['geo_enabled'] == false) ? 0 : 1;

						if (empty($retweetedUserData)) {	//RTされたユーザーが登録されていなかった場合
							$stmt = mysqli_prepare($mysqlConnect, "INSERT INTO users (twitter_id_str, screen_name, name, description, url, favourites_count, protected, friends_count, followers_count, language, verified, statuses_count, profile_image_url, profile_background_image_url, geo_enabled, location, created, modified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,? , ?, ?, ?, ?)");
							mysqli_stmt_bind_param($stmt, 'sssssiiiisiississs',
								$userData['id_str'],
								$userData['screen_name'],
								$userData['name'],
								$userData['description'],
								$userData['url'],
								$userData['favourites_count'],
								$userData['protected'],
								$userData['friends_count'],
								$userData['followers_count'],
								$userData['lang'],
								$userData['verified'],
								$userData['statuses_count'],
								$userData['profile_image_url'],
								$userData['profile_background_image_url'],
								$userData['geo_enabled'],
								$userData['location'],
								date( 'Y-m-d H:i:s', strtotime($userData['created_at'])),
								date( 'Y-m-d H:i:s', strtotime($userData['created_at']))
							);
						} else {
							$stmt = mysqli_prepare($mysqlConnect, "UPDATE users SET
								screen_name = ?,
								name = ?,
								description = ?,
								url = ?,
								favourites_count = ?,
								protected = ?,
								friends_count = ?,
								followers_count = ?,
								language = ?,
								verified = ?,
								statuses_count = ?,
								profile_image_url = ?,
								profile_background_image_url = ?,
								geo_enabled = ?,
								location = ?,
								modified = ?
								WHERE twitter_id_str = ?");

							mysqli_stmt_bind_param($stmt, 'ssssiiiisiississs',
								$userData['screen_name'],
								$userData['name'],
								$userData['description'],
								$userData['url'],
								$userData['favourites_count'],
								$userData['protected'],
								$userData['friends_count'],
								$userData['followers_count'],
								$userData['lang'],
								$userData['verified'],
								$userData['statuses_count'],
								$userData['profile_image_url'],
								$userData['profile_background_image_url'],
								$userData['geo_enabled'],
								$userData['location'],
								date( 'Y-m-d H:i:s', time()),
								$userData['id_str']
							);
						}

						if (! mysqli_stmt_execute($stmt)) {
							exit("Error 1: " . mysqli_stmt_error($stmt) . "\n");
						}
						mysqli_stmt_close($stmt);
						$retweetedUserData = mysqli_fetch_assoc(mysqli_query($mysqlConnect, "SELECT id, twitter_id_str, screen_name, disabled FROM users WHERE twitter_id_str = '{$userData['id_str']}';"));	//RTされたユーザーID(users.id)を取得するため

						if ($existRetweet['sum'] == 0) {	//tweets tableにRTされたツイートが登録されていなかった場合
							$stmt = mysqli_prepare($mysqlConnect, "INSERT INTO tweets (tweet_id_str, user_id, text, retweet_count, favorite_count, source, country, full_name, created, modified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
							mysqli_stmt_bind_param($stmt, 'sisiisssss',
								$retweetData['id_str'],
								$retweetedUserData['id'],
								$retweetData['text'],
								$retweetData['retweet_count'],
								$retweetData['favorite_count'],
								$retweetData['source'],
								$retweetDataCountry,
								$retweetDataFullName,
								date( 'Y-m-d H:i:s', strtotime($retweetData['created_at'])),
								date( 'Y-m-d H:i:s', strtotime($tweetData['created_at']))
							);
							$retweetDataCountry = ($retweetData['place'] == false) ? null : $retweetData['place']['country'];
							$retweetDataFullName = ($retweetData['place'] == false) ? null : $retweetData['place']['full_name'];
						} else {	//tweets tableにRTされたツイートが登録されていた場合
							$stmt = mysqli_prepare($mysqlConnect, "UPDATE tweets SET retweet_count = ?, favorite_count = ?,	modified = ? WHERE user_id = ?");
							mysqli_stmt_bind_param($stmt, 'iisi',
								$retweetData['retweet_count'],
								$retweetData['favorite_count'],
								date( 'Y-m-d H:i:s', strtotime($tweetData['created_at'])),
								$retweetedUserData['id']
							);
						}

						if (! mysqli_stmt_execute($stmt)) {
							exit("Error 2: " . mysqli_stmt_error($stmt) . "\n");
						}
						mysqli_stmt_close($stmt);
					}

					/* Userを登録 */
					$userData = $tweetData['user'];
					$existUser = mysqli_fetch_array(mysqli_query($mysqlConnect, "SELECT count(*) AS sum FROM users WHERE twitter_id_str = '{$userData['id_str']}';"), MYSQLI_ASSOC);

					//mysqlにはfalse -> 0，true -> 1を入れたい
					$userData['protected'] = ($userData['protected'] == false) ? 0 : 1;
					$userData['verified'] = ($userData['verified'] == false) ? 0 : 1;
					$userData['geo_enabled'] = ($userData['geo_enabled'] == false) ? 0 : 1;

					if ($existUser['sum'] == 0) {	//ユーザーが登録されていなかった場合
						$stmt = mysqli_prepare($mysqlConnect, "INSERT INTO users (twitter_id_str, screen_name, name, description, url, favourites_count, protected, friends_count, followers_count, language, verified, statuses_count, profile_image_url, profile_background_image_url, geo_enabled, location, created, modified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
						mysqli_stmt_bind_param($stmt, 'sssssiiiisiississs',
							$userData['id_str'],
							$userData['screen_name'],
							$userData['name'],
							$userData['description'],
							$userData['url'],
							$userData['favourites_count'],
							$userData['protected'],
							$userData['friends_count'],
							$userData['followers_count'],
							$userData['lang'],
							$userData['verified'],
							$userData['statuses_count'],
							$userData['profile_image_url'],
							$userData['profile_background_image_url'],
							$userData['geo_enabled'],
							$userData['location'],
							date( 'Y-m-d H:i:s', strtotime($userData['created_at'])),
							date( 'Y-m-d H:i:s', strtotime($userData['created_at']))
						);
					} else {
						$stmt = mysqli_prepare($mysqlConnect, "UPDATE users SET
							screen_name = ?,
							name = ?,
							description = ?,
							url = ?,
							favourites_count = ?,
							protected = ?,
							friends_count = ?,
							followers_count = ?,
							language = ?,
							verified = ?,
							statuses_count = ?,
							profile_image_url = ?,
							profile_background_image_url = ?,
							geo_enabled = ?,
							location = ?,
							modified = ?
							WHERE twitter_id_str = ?"
						);

						mysqli_stmt_bind_param($stmt, 'ssssiiiisiississs',
							$userData['screen_name'],
							$userData['name'],
							$userData['description'],
							$userData['url'],
							$userData['favourites_count'],
							$userData['protected'],
							$userData['friends_count'],
							$userData['followers_count'],
							$userData['lang'],
							$userData['verified'],
							$userData['statuses_count'],
							$userData['profile_image_url'],
							$userData['profile_background_image_url'],
							$userData['geo_enabled'],
							$userData['location'],
							date( 'Y-m-d H:i:s', time()),
							$userData['id_str']
						);
					}

					if (! mysqli_stmt_execute($stmt)) {
						exit("Error 3: " . mysqli_stmt_error($stmt) . "\n" . "query : ");
					}
					mysqli_stmt_close($stmt);

					/* Tweetを登録 */
					$tweetUserData = mysqli_fetch_assoc(mysqli_query($mysqlConnect, "SELECT id, twitter_id_str, screen_name, disabled FROM users WHERE twitter_id_str = '{$userData['id_str']}';"));	//RTされたユーザーID(users.id)を取得するため
					$existTweet = mysqli_fetch_array(mysqli_query($mysqlConnect, "SELECT count(*) AS sum FROM tweets WHERE tweet_id_str = '{$tweetData['id_str']}';"), MYSQLI_ASSOC);

					if ($existTweet['sum'] == 0) {	//tweets tableにツイートが登録されていなかった場合
						$stmt = mysqli_prepare($mysqlConnect, "INSERT INTO tweets (tweet_id_str, user_id, text, retweet_count, favorite_count, source, country, full_name, created, modified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
						mysqli_stmt_bind_param($stmt, 'sisiisssss',
							$tweetData['id_str'],
							$tweetUserData['id'],
							$tweetData['text'],
							$tweetData['retweet_count'],
							$tweetData['favorite_count'],
							$tweetData['source'],
							$tweetDataCountry,
							$tweetDataFullName,
							date( 'Y-m-d H:i:s', strtotime($tweetData['created_at'])),
							date( 'Y-m-d H:i:s', strtotime($tweetData['created_at']))
						);
						$tweetDataCountry = ($tweetData['place'] == false) ? null : $tweetData['place']['country'];
						$tweetDataFullName = ($tweetData['place'] == false) ? null : $tweetData['place']['full_name'];
					} else {	//tweets tableにツイートが登録されていた場合
						$stmt = mysqli_prepare($mysqlConnect, "UPDATE tweets SET retweet_count = ?, favorite_count = ?,	modified = ? WHERE user_id = ?");
						mysqli_stmt_bind_param($stmt, 'iisi',
							$tweetData['retweet_count'],
							$tweetData['favorite_count'],
							date( 'Y-m-d H:i:s', time()),
							$tweetUserData['id']
						);
					}

					if (! mysqli_stmt_execute($stmt)) {
						exit("Error 4: " . mysqli_stmt_error($stmt) . "\n");
					}
					mysqli_stmt_close($stmt);
				} else if (is_array($tweetData) && array_key_exists('delete', $tweetData)) {
					$deleteDate = date('Y-m-d H:i:s', (int)($tweetData['delete']['timestamp_ms']/1000));

					/* ツイートの削除処理 */
					$query =
						"UPDATE tweets SET
						modified = '{$deleteDate}',
						disabled = 1
						WHERE tweet_id_str = '{$tweetData['delete']['status']['id_str']}';";

					if (! mysqli_query($mysqlConnect, $query)) {
						exit("mysql query ERROR 5\n QUERY : $query");
					}
				}
				mysqli_close($mysqlConnect);
			} else {
				exit("not found database\n");
			}
		} catch (Exception $e) {
			echo '捕捉した例外: ',  $e->getMessage(), "\n";
		}
	}
}

//These are the application key and secret
//You can create an application, and then get this info, from https://dev.twitter.com/apps
//(They are under OAuth Settings, called "Consumer key" and "Consumer secret")
define('TWITTER_CONSUMER_KEY', 'XXXXXXXXXXXXXXXXXXXXXXXXX');
define('TWITTER_CONSUMER_SECRET', 'XXXXXXXXXXXXXXXXXXXXXXXXX');

//These are the user's token and secret
//You can get this from https://dev.twitter.com/apps, under the "Your access token"
//section for your app.
define('OAUTH_TOKEN', 'XXXXXXXXXXXXXXXXXXXXXXXXX');
define('OAUTH_SECRET', 'XXXXXXXXXXXXXXXXXXXXXXXXX');

// Start streaming
$sc = new MyUserConsumer(OAUTH_TOKEN, OAUTH_SECRET, Phirehose::METHOD_USER);
$sc->consume();

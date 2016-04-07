<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace Doctrine\MongoDB;

use Doctrine\Common\EventManager;
use Doctrine\MongoDB\Event\EventArgs;

/**
 * Wrapper for the MongoDB\Client class.
 *
 * @since  1.0
 * @author Jonathan H. Wage <jonwage@gmail.com>
 */
class Connection
{
    /**
     * The PHP \MongoDB\Client instance being wrapped.
     *
     * @var \MongoDB\Client
     */
    protected $mongoClient;

    /**
     * Server string used to construct the \MongoDB\Client instance (optional).
     *
     * @var string
     */
    protected $server;

    /**
     * Options used to construct the \MongoDB\Client instance (optional).
     *
     * @var array
     */
    protected $options = array();

    /**
     * The Configuration for this connection.
     *
     * @var Configuration
     */
    protected $config;

    /**
     * The EventManager used to dispatch events.
     *
     * @var \Doctrine\Common\EventManager
     */
    protected $eventManager;

    /**
     * Constructor.
     *
     * If $server is an existing \MongoDB\Client instance, the $options parameter
     * will not be used.
     *
     * @param string|\MongoDB\Client $server  Server string or \MongoDB\Client instance
     * @param array               $options \MongoDB\Client constructor options
     * @param Configuration       $config  Configuration instance
     * @param EventManager        $evm     EventManager instance
     */
    public function __construct($server = null, array $options = array(), Configuration $config = null, EventManager $evm = null)
    {
        if ($server instanceof \MongoDB\Client || $server instanceof \Mongo) {
            $this->mongoClient = $server;
        } else {
            $this->server = $server;
            $this->options = $options;
        }
        $this->config = $config ? $config : new Configuration();
        $this->eventManager = $evm ? $evm : new EventManager();
    }

    /**
     * Wrapper method for \MongoDB\Client::close().
     *
     * @see http://php.net/manual/en/mongoclient.close.php
     * @return boolean
     */
    public function close()
    {
        $this->initialize();
        return $this->mongoClient->close();
    }

    /**
     * Wrapper method for \MongoDB\Client::connect().
     *
     * @see http://php.net/manual/en/mongoclient.connect.php
     * @return boolean
     */
    public function connect()
    {
        $this->initialize();

        $mongoClient = $this->mongoClient;
        return $this->retry(function() use ($mongoClient) {
            return $mongoClient->connect();
        });
    }

    /**
     * Wrapper method for \MongoDB\Client::dropDB().
     *
     * This method will dispatch preDropDatabase and postDropDatabase events.
     *
     * @see http://php.net/manual/en/mongoclient.dropdb.php
     * @param string $database
     * @return array
     */
    public function dropDatabase($database)
    {
        if ($this->eventManager->hasListeners(Events::preDropDatabase)) {
            $this->eventManager->dispatchEvent(Events::preDropDatabase, new EventArgs($this, $database));
        }

        $this->initialize();
        $result = $this->mongoClient->dropDB($database);

        if ($this->eventManager->hasListeners(Events::postDropDatabase)) {
            $this->eventManager->dispatchEvent(Events::postDropDatabase, new EventArgs($this, $result));
        }

        return $result;
    }

    /**
     * Get the Configuration used by this Connection.
     *
     * @return Configuration
     */
    public function getConfiguration()
    {
        return $this->config;
    }

    /**
     * Get the EventManager used by this Connection.
     *
     * @return \Doctrine\Common\EventManager
     */
    public function getEventManager()
    {
        return $this->eventManager;
    }

    /**
     * Get the \MongoDB\Client instance being wrapped.
     *
     * @deprecated 1.1 Replaced by getMongoClient(); will be removed for 2.0
     * @return \MongoDB\Client
     */
    public function getMongo()
    {
        return $this->getMongoClient();
    }

    /**
     * Set the \MongoDB\Client instance to wrap.
     *
     * @deprecated 1.1 Will be removed for 2.0
     * @param \MongoDB\Client $mongoClient
     */
    public function setMongo($mongoClient)
    {
        if ( ! ($mongoClient instanceof \MongoDB\Client || $mongoClient instanceof \Mongo)) {
            throw new \InvalidArgumentException('\MongoDB\Client or Mongo instance required');
        }

        $this->mongoClient = $mongoClient;
    }

    /**
     * Get the \MongoDB\Client instance being wrapped.
     *
     * @return \MongoDB\Client
     */
    public function getMongoClient()
    {
        $this->initialize();
        return $this->mongoClient;
    }

    /**
     * Wrapper method for \MongoDB\Client::getReadPreference().
     *
     * For driver versions between 1.3.0 and 1.3.3, the return value will be
     * converted for consistency with {@link Connection::setReadPreference()}.
     *
     * @see http://php.net/manual/en/mongoclient.getreadpreference.php
     * @return array
     */
    public function getReadPreference()
    {
        $this->initialize();
        return $this->mongoClient->getReadPreference();
    }

    /**
     * Wrapper method for \MongoDB\Client::setReadPreference().
     *
     * @see http://php.net/manual/en/mongoclient.setreadpreference.php
     * @param string $readPreference
     * @param array  $tags
     * @return boolean
     */
    public function setReadPreference($readPreference, array $tags = null)
    {
        $this->initialize();
        if (isset($tags)) {
            return $this->mongoClient->setReadPreference($readPreference, $tags);
        }

        return $this->mongoClient->setReadPreference($readPreference);
    }

    /**
     * Get the server string.
     *
     * @return string|null
     */
    public function getServer()
    {
        return $this->server;
    }

    /**
     * Gets the $status property of the wrapped \MongoDB\Client instance.
     *
     * @deprecated 1.1 No longer used in driver; Will be removed for 1.2
     * @return string
     */
    public function getStatus()
    {
        $this->initialize();
        if ( ! ($this->mongoClient instanceof \MongoDB\Client || $this->mongoClient instanceof \Mongo)) {
            return null;
        }

        return $this->mongoClient->status;
    }

    /**
     * Construct the wrapped \MongoDB\Client instance if necessary.
     *
     * This method will dispatch preConnect and postConnect events.
     */
    public function initialize()
    {
        if ($this->mongoClient !== null) {
            return;
        }

        if ($this->eventManager->hasListeners(Events::preConnect)) {
            $this->eventManager->dispatchEvent(Events::preConnect, new EventArgs($this));
        }

        $server = $this->server ?: 'mongodb://localhost:27017';
        $options = $this->options;

        $options = isset($options['timeout']) ? $this->convertConnectTimeout($options) : $options;
        $options = isset($options['wTimeout']) ? $this->convertWriteTimeout($options) : $options;

        $this->mongoClient = $this->retry(function() use ($server, $options) {
            return new \MongoDB\Client($server, $options);
        });

        if ($this->eventManager->hasListeners(Events::postConnect)) {
            $this->eventManager->dispatchEvent(Events::postConnect, new EventArgs($this));
        }
    }

    /**
     * Checks whether the connection is initialized and connected.
     *
     * @return boolean
     */
    public function isConnected()
    {
        if ( ! ($this->mongoClient instanceof \MongoDB\Client || $this->mongoClient instanceof \Mongo)) {
            return false;
        }

        return count($this->mongoClient->getHosts()) > 0;
    }

    /**
     * Wrapper method for \MongoDB\Client::listDBs().
     *
     * @see http://php.net/manual/en/mongoclient.listdbs.php
     * @return array
     */
    public function listDatabases()
    {
        $this->initialize();
        return $this->mongoClient->listDBs();
    }

    /**
     * Log something using the configured logger callable (if available).
     *
     * @param array $log
     */
    public function log(array $log)
    {
        if (null !== $this->config->getLoggerCallable()) {
            call_user_func_array($this->config->getLoggerCallable(), array($log));
        }
    }

    /**
     * Wrapper method for \MongoDB\Client::selectCollection().
     *
     * @see http://php.net/manual/en/mongoclient.selectcollection.php
     * @param string $db
     * @param string $collection
     * @return Collection
     */
    public function selectCollection($db, $collection)
    {
        $this->initialize();
        return $this->selectDatabase($db)->selectCollection($collection);
    }

    /**
     * Wrapper method for \MongoDB\Client::selectDatabase().
     *
     * This method will dispatch preSelectDatabase and postSelectDatabase
     * events.
     *
     * @see http://php.net/manual/en/mongoclient.selectdatabase.php
     * @param string $name
     * @return Database
     */
    public function selectDatabase($name)
    {
        if ($this->eventManager->hasListeners(Events::preSelectDatabase)) {
            $this->eventManager->dispatchEvent(Events::preSelectDatabase, new EventArgs($this, $name));
        }

        $this->initialize();
        $database = $this->doSelectDatabase($name);

        if ($this->eventManager->hasListeners(Events::postSelectDatabase)) {
            $this->eventManager->dispatchEvent(Events::postSelectDatabase, new EventArgs($this, $database));
        }

        return $database;
    }

    /**
     * Wrapper method for \MongoDB\Client::__get().
     *
     * @see http://php.net/manual/en/mongoclient.get.php
     * @param string $database
     * @return \MongoDB
     */
    public function __get($database)
    {
        $this->initialize();
        return $this->mongoClient->__get($database);
    }

    /**
     * Wrapper method for \MongoDB\Client::__toString().
     *
     * @see http://php.net/manual/en/mongoclient.tostring.php
     * @return string
     */
    public function __toString()
    {
        $this->initialize();
        return $this->mongoClient->__toString();
    }

    /**
     * Return a new Database instance.
     *
     * If a logger callable was defined, a LoggableDatabase will be returned.
     *
     * @see Connection::selectDatabase()
     * @param string $name
     * @return Database
     */
    protected function doSelectDatabase($name)
    {
        $mongoDB = $this->mongoClient->selectDatabase($name);
        $numRetries = $this->config->getRetryQuery();
        $loggerCallable = $this->config->getLoggerCallable();

        return $loggerCallable !== null
            ? new LoggableDatabase($this, $mongoDB, $this->eventManager, $numRetries, $loggerCallable)
            : new Database($this, $mongoDB, $this->eventManager, $numRetries);
    }

    /**
     * Conditionally retry a closure if it yields an exception.
     *
     * If the closure does not return successfully within the configured number
     * of retries, its first exception will be thrown.
     *
     * @param \Closure $retry
     * @return mixed
     */
    protected function retry(\Closure $retry)
    {
        $numRetries = $this->config->getRetryConnect();

        if ($numRetries < 1) {
            return $retry();
        }

        $firstException = null;

        for ($i = 0; $i <= $numRetries; $i++) {
            try {
                return $retry();
            } catch (\MongoException $e) {
                if ($firstException === null) {
                    $firstException = $e;
                }
                if ($i === $numRetries) {
                    throw $firstException;
                }
            }
        }
    }

    /**
     * Converts "timeout" \MongoDB\Client constructor option to "connectTimeoutMS"
     * for driver versions 1.4.0+.
     *
     * Note: \MongoDB\Client actually allows case-insensitive option names, but
     * we'll only process the canonical version here.
     *
     * @param array $options
     * @return array
     */
    protected function convertConnectTimeout(array $options)
    {
        if (isset($options['timeout']) && ! isset($options['connectTimeoutMS'])) {
            $options['connectTimeoutMS'] = $options['timeout'];
            unset($options['timeout']);
        }

        return $options;
    }

    /**
     * Converts "wTimeout" \MongoDB\Client constructor option to "wTimeoutMS" for
     * driver versions 1.4.0+.
     *
     * Note: \MongoDB\Client actually allows case-insensitive option names, but
     * we'll only process the canonical version here.
     *
     * @param array $options
     * @return array
     */
    protected function convertWriteTimeout(array $options)
    {
        if (isset($options['wTimeout']) && ! isset($options['wTimeoutMS'])) {
            $options['wTimeoutMS'] = $options['wTimeout'];
            unset($options['wTimeout']);
        }

        return $options;
    }
}

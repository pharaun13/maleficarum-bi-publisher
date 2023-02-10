<?php
/**
 * This class is responsible for maintaining a registry of active rabbitmq connections that will be shared across multiple objects.
 */
declare (strict_types=1);

namespace Maleficarum\Rabbitmq\Manager;

class Manager {
    /* ------------------------------------ Class Property START --------------------------------------- */

    /**
     * Test prefix used for test mode connectionIdentifier creation.
     */
    const TEST_PREFIX = 'test_';
    
    /**
     * Internal storage for available RabbitMQ connections. 
     * @var array 
     */
    private array $connections = [];
    
    /* ------------------------------------ Class Property END ----------------------------------------- */
    
    /* ------------------------------------ Class Methods START ---------------------------------------- */

    /**
     * Add a new connection to the connection pool.
     * 
     * @param \Maleficarum\Rabbitmq\Connection\Connection $connection
     * @param string $identifier
     * @param int $mode
     * @param int $priority
     * @throws \InvalidArgumentException
     * @return \Maleficarum\Rabbitmq\Manager\Manager
     */
    protected function addConnection(\Maleficarum\Rabbitmq\Connection\Connection $connection, string $identifier) : \Maleficarum\Rabbitmq\Manager\Manager {
        // detect identifier duplication
        if (array_key_exists($identifier, $this->connections)) throw new \InvalidArgumentException(sprintf('Duplicate connection identifier. %s()', __METHOD__));
        
        // create the connection definition structure based on the input parameters.
        $this->connections[$identifier] = [
            'connection' => $connection,
        ];
        
        return $this;
    }

    /**
     * Add a new command to a specified connection.
     * 
     * @param \Maleficarum\Command\AbstractCommand $command
     * @param string $connectionIdentifier
     * @param array $commandHeaders
     * @return \Maleficarum\Rabbitmq\Manager\Manager
     *
     *@throws \InvalidArgumentException
     */
    protected function addCommand(\Maleficarum\Command\AbstractCommand $command, string $connectionIdentifier, array $commandHeaders = []) : \Maleficarum\Rabbitmq\Manager\Manager {
        // set test connectionIdentifier
        $connectionIdentifier = $this->getConnectionIdentifier($command, $connectionIdentifier);

        // check if the specified connection identifier exists
        if (!array_key_exists($connectionIdentifier, $this->connections)) throw new \InvalidArgumentException(sprintf('Provided connection identifier does not exist. %s', __METHOD__));
        
        // recover the specified connection for internal storage
        $connection = $this->connections[$connectionIdentifier]['connection'];

        // initialise the connection if necessary
        $connection->connect();

        $applicationHeaders = \Maleficarum\Ioc\Container::get('PhpAmqpLib\Wire\AMQPTable', [$commandHeaders]);

        // send the command to the message broker
        $message = \Maleficarum\Ioc\Container::get(
            'PhpAmqpLib\Message\AMQPMessage',
            [$command->toJSON(), ['delivery_mode' => 2, 'application_headers' => $applicationHeaders]]);
        $channel = $connection->getChannel();
        $channel->basic_publish($message, $connection->getExchangeName(), $connection->getQueueName());
        $channel->close();

        // close the connection if it's in transient mode
        'transient' === $this->connections[$connectionIdentifier]['mode'] and $connection->disconnect();
        
        return $this;
    }

    /**
     * Get connectionIdentifier based on current testMode.
     *
     * @param \Maleficarum\Command\AbstractCommand $command
     * @param string $connectionIdentifier
     * @return string
     */
    private function getConnectionIdentifier(\Maleficarum\Command\AbstractCommand $command, string $connectionIdentifier): string {
        $command->getTestMode() and $connectionIdentifier = self::TEST_PREFIX . $connectionIdentifier;
        return $connectionIdentifier;
    }

    /* ------------------------------------ Class Methods END ------------------------------------------ */
}

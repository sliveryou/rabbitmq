package rabbitmq

import (
	"errors"

	"github.com/streadway/amqp"
)

// Config represents a RabbitMQ AMQP URI config.
type Config struct {
	Host     string
	Port     int
	Username string
	Password string
	Vhost    string
}

// New returns a new *RabbitMQ which contains a new AMQP connection by config.
func New(c Config) (*RabbitMQ, error) {
	r := &RabbitMQ{
		config: &c,
	}

	if err := r.Dial(); err != nil {
		return nil, err
	}

	return r, nil
}

// RabbitMQ represents a AMQP connection with its URI config.
type RabbitMQ struct {
	connection *Connection
	config     *Config
}

// Connection returns the AMQP connection.
func (r *RabbitMQ) Connection() *Connection {
	return r.connection
}

// SetDelay sets delay retry seconds for the AMQP connection.
func (r *RabbitMQ) SetDelay(seconds int) {
	r.connection.SetDelay(seconds)
}

// Dial dials a new AMQP connection over TCP using the AMQP URI config.
func (r *RabbitMQ) Dial() error {
	if r.config == nil {
		return errors.New("mq config is nil")
	}

	conf := amqp.URI{
		Scheme:   "amqp",
		Host:     r.config.Host,
		Port:     r.config.Port,
		Username: r.config.Username,
		Password: r.config.Password,
		Vhost:    r.config.Vhost,
	}.String()

	var err error
	r.connection, err = Dial(conf)
	if err != nil {
		return err
	}

	return nil
}

// Connect dials if the AMQP connection is nil, otherwise returns nil.
func (r *RabbitMQ) Connect() error {
	if r.connection != nil {
		return nil
	}

	if err := r.Dial(); err != nil {
		return err
	}

	return nil
}

// Close closes the AMQP connection.
func (r *RabbitMQ) Close() error {
	return r.connection.Close()
}

// Exchange represents exchange declare config.
type Exchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

// Queue represents queue declare config.
type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

// BindOptions represents queue bind options.
type BindOptions struct {
	BindingKey string
	NoWait     bool
	Args       amqp.Table
}

// ConsumeOptions represents consumer consume options.
type ConsumeOptions struct {
	Tag       string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

// PublishOptions represents producer publish options.
type PublishOptions struct {
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

// Session represents executer configs and options.
type Session struct {
	Exchange       Exchange
	Queue          Queue
	BindOptions    BindOptions
	ConsumeOptions ConsumeOptions
	PublishOptions PublishOptions
}

// executer represents a AMQP channel with its session.
type executer struct {
	channel *Channel
	session *Session
}

// SetDelay sets delay retry seconds for the AMQP channel.
func (e *executer) SetDelay(seconds int) {
	e.channel.SetDelay(seconds)
}

// Close closes the AMQP channel.
func (e *executer) Close() error {
	return e.channel.Close()
}

// ExchangeDeclare declares an exchange on the server. If the exchange does not
// already exist, the server will create it. If the exchange exists, the server
// verifies that it is of the provided type, durability and auto-delete flags.
func (e *executer) ExchangeDeclare() error {
	exchange := e.session.Exchange

	return e.channel.ExchangeDeclare(
		exchange.Name,
		exchange.Type,
		exchange.Durable,
		exchange.AutoDelete,
		exchange.Internal,
		exchange.NoWait,
		exchange.Args,
	)
}

// QueueDeclare declares a queue to hold messages and deliver to consumers.
// Declaring creates a queue if it doesn't already exist, or ensures that an
// existing queue matches the same parameters.
func (e *executer) QueueDeclare() error {
	queue := &e.session.Queue

	q, err := e.channel.QueueDeclare(
		queue.Name,
		queue.Durable,
		queue.AutoDelete,
		queue.Exclusive,
		queue.NoWait,
		queue.Args,
	)

	if err != nil {
		return err
	}

	if queue.Name == "" {
		queue.Name = q.Name
	}

	return nil
}

// QueueBind binds an exchange to a queue so that publishings to the exchange will
// be routed to the queue when the publishing routing key matches the binding
// routing key.
func (e *executer) QueueBind() error {
	queueName := e.session.Queue.Name
	exchangeName := e.session.Exchange.Name
	bindOptions := e.session.BindOptions

	return e.channel.QueueBind(
		queueName,
		bindOptions.BindingKey,
		exchangeName,
		bindOptions.NoWait,
		bindOptions.Args,
	)
}

// DeclareAndBind declares an exchange and a queue, then binds the exchange to the queue.
func (e *executer) DeclareAndBind() error {
	var err error

	if err = e.ExchangeDeclare(); err != nil {
		return err
	}

	if err = e.QueueDeclare(); err != nil {
		return err
	}

	if err = e.QueueBind(); err != nil {
		return err
	}

	return nil
}

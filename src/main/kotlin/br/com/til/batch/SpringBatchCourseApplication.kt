package br.com.til.batch

import org.quartz.*
import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.explore.JobExplorer
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.job.flow.JobExecutionDecider
import org.springframework.batch.core.job.flow.support.SimpleFlow
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.database.ItemPreparedStatementSetter
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.JdbcPagingItemReader
import org.springframework.batch.item.database.PagingQueryProvider
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.mapping.DefaultLineMapper
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor
import org.springframework.batch.item.file.transform.DelimitedLineAggregator
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder
import org.springframework.batch.item.support.CompositeItemProcessor
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder
import org.springframework.batch.item.validator.BeanValidatingItemProcessor
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.core.io.FileSystemResource
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.quartz.QuartzJobBean
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
class SpringBatchCourseApplication(
	private val jobBuilderFactory: JobBuilderFactory,
	private val stepBuilderFactory: StepBuilderFactory,
	private val jobLauncher: JobLauncher,
	private val jobExplorer: JobExplorer,
	private val dataSource: DataSource
) : QuartzJobBean() {

	private val tokens = arrayOf("order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date")

	private val names = arrayOf("orderId", "firstName", "lastName", "email", "cost", "itemId", "itemName", "shipDate")

	companion object {
		const val ORDER_SQL =
			"""select 
				order_id, 
				first_name, 
				last_name, 
				email, 
				cost, 
				item_id, 
				item_name, 
				ship_date 
			from SHIPPED_ORDER 
			order by order_id"""

		const val INSERT_ORDER_SQL =
			"""insert into
				SHIPPED_ORDER_OUTPUT(
				order_id, 
				first_name, 
				last_name, 
				email, 
				item_id, 
				item_name, 
				cost, 
				ship_date)
				values(:orderId, :firstName, :lastName, :email, :itemId, :itemName, :cost, :shipDate)"""
	}
	@Bean
	fun decider() : JobExecutionDecider {
		return DeliveryDecider()
	}

	@Bean
	fun receiptDecider() : JobExecutionDecider {
		return ReceiptDecider()
	}

	@Bean
	fun thanksCustomerStep() : Step {
		val tasklet = Tasklet { contribution, chunkContext ->

			println("Thanking the customer.")
			RepeatStatus.FINISHED
		}

		return stepBuilderFactory.get("thanksCustomerStep").tasklet(tasklet).build()
	}

	@Bean
	fun refundStep() : Step {
		val tasklet = Tasklet { contribution, chunkContext ->

			println("Refunding customer money.")
			RepeatStatus.FINISHED
		}

		return stepBuilderFactory.get("refundStep").tasklet(tasklet).build()
	}

	@Bean
	fun leaveAtDoorStep() : Step {
		val tasklet = Tasklet { contribution, chunkContext ->

			println("We leavind the package at the door.")
			RepeatStatus.FINISHED
		}

		return stepBuilderFactory.get("leaveAtDoorStep").tasklet(tasklet).build()
	}

	@Bean
	fun storePackageStep() : Step {
		val tasklet = Tasklet { contribution, chunkContext ->

			println("Storing the package while the customer address is located.")
			RepeatStatus.FINISHED
		}

		return stepBuilderFactory.get("storePackageStep").tasklet(tasklet).build()
	}

	@Bean
	fun givePackageToCustomerStep() : Step {
		val tasklet = Tasklet { contribution, chunkContext ->

			println("Given the package to the customer.")
			RepeatStatus.FINISHED
		}

		return stepBuilderFactory.get("givePackageToCustomerStep").tasklet(tasklet).build()
	}

	@Bean
	fun driveToAddressStep() : Step {
		val gotLost = false
		val tasklet = Tasklet { contribution, chunkContext ->

			if(gotLost) throw RuntimeException("Got lost driving to the address.")

			println("Successfulle arrived the address.")
			RepeatStatus.FINISHED
		}

		return stepBuilderFactory.get("driveToAddressStep").tasklet(tasklet).build()
	}

	@Bean
	fun packageItemStep(): Step {

		val taskLet = Tasklet { contribution, chunkContext ->

			val item = chunkContext.stepContext.jobParameters["item"].toString()

			val date = chunkContext.stepContext.jobParameters["run.date"].toString()

			println("The $item has been packaged on $date.")
			RepeatStatus.FINISHED
		}

		return stepBuilderFactory.get("packageItemStep").tasklet(taskLet).build()
	}

	fun deliveryFlow() : Flow {
		return FlowBuilder<SimpleFlow>("deliveryFlow")
			.start(driveToAddressStep())
				.on("FAILED").fail()//to(storePackageStep())
			.from(driveToAddressStep())
				.on("*").to(decider())
				.on("PRESENT").to(givePackageToCustomerStep())
					.next(receiptDecider()).on("CORRECT").to(thanksCustomerStep())
					.from(receiptDecider()).on("INCORRECT").to(refundStep())
			.from(decider())
				.on("NOT_PRESENT").to(leaveAtDoorStep())
			.build()
	}

	@Bean
	fun deliverPackageJop() : Job {
		return jobBuilderFactory.get("deliveryPackageJob")
			.start(packageItemStep())
				.on("*").to(deliveryFlow())
			.end()
			.build()
	}

	@Bean
	fun job() : Job {
		return jobBuilderFactory.get("jobChunk")
			.start(chunkBasedStep())
			.build()
	}

	@Bean
	fun itemReaderFlatFileItemReader(): ItemReader<Order> {
		val itemreader = FlatFileItemReader<Order>()

		itemreader.setLinesToSkip(1)
		itemreader.setResource(FileSystemResource("data/shipped_orders.csv"))

		val lineMapper = DefaultLineMapper<Order>()

		val tokenizer = DelimitedLineTokenizer()

		tokenizer.setNames(*tokens)

		lineMapper.setLineTokenizer(tokenizer)
		lineMapper.setFieldSetMapper(OrderFieldSetMapper())

		itemreader.setLineMapper(lineMapper)
		return itemreader
	}

	@Bean
	fun itemReaderSimpleItemReader(): ItemReader<String> {
		return SimpleItemReader()
	}

	@Bean
	fun itemReaderJdbcCursorItemReader(): ItemReader<Order> {
		return JdbcCursorItemReaderBuilder<Order>()
			.dataSource(dataSource)
			.name("jdbcCursorItemReader")
			.sql(ORDER_SQL)
			.rowMapper(OrderRowMapper())
			.build()
	}

	@Bean
	fun itemReaderJdbcPagingItemReader(): ItemReader<Order> {
		return JdbcPagingItemReaderBuilder<Order>()
			.dataSource(dataSource)
			.name("jdbcCursorItemReader")
			.queryProvider(queryProvider())
			.rowMapper(OrderRowMapper())
			.build()
	}

	@Bean
	fun queryProvider(): PagingQueryProvider {
		val factory = SqlPagingQueryProviderFactoryBean()

		factory.setSelectClause(
			"""select 
				order_id, 
				first_name, 
				last_name, 
				email, 
				cost, 
				item_id, 
				item_name, 
				ship_date"""
		)
		factory.setFromClause("from SHIPPED_ORDER")
		factory.setSortKey("order_id")
		factory.setDataSource(dataSource)

		return factory.`object`
	}

	@Bean
	fun flatFileItemWriter(): ItemWriter<Order> {
		val itemWriter = FlatFileItemWriter<Order>()

		itemWriter.setResource(FileSystemResource("data/shipped_orders_output.csv"))

		val aggregator = DelimitedLineAggregator<Order>()

		aggregator.setDelimiter(",")

		val fieldExtractor = BeanWrapperFieldExtractor<Order>()

		fieldExtractor.setNames(names)

		aggregator.setFieldExtractor(fieldExtractor)

		itemWriter.setLineAggregator(aggregator)

		return itemWriter
	}

	@Bean
	fun jdbcBatchItemWriter(): ItemWriter<Order> {
		return JdbcBatchItemWriterBuilder<Order>()
			.dataSource(dataSource)
			.sql(INSERT_ORDER_SQL)
			//.itemPreparedStatementSetter(OrderItemPreparedStatementSetter())
			.beanMapped()
			.build()
	}

	@Bean
	fun jsonFileItemWriter(): ItemWriter<TrackedOrder> {
		return JsonFileItemWriterBuilder<TrackedOrder>()
			.jsonObjectMarshaller(JacksonJsonObjectMarshaller<TrackedOrder>())
			.resource(FileSystemResource("data/shipped_orders_output.json"))
			.name("jsonItemWriter")
			.build()
	}

	@Bean
	fun orderValidationItemProcessor(): ItemProcessor<Order, Order> {
		val itemProcessor = BeanValidatingItemProcessor<Order>()

		itemProcessor.setFilter(true)
		return itemProcessor
	}

	@Bean
	fun trackedOrderItemProcessor() : ItemProcessor<Order, TrackedOrder> {
		return TrackedOrderItemProcessor()
	}

	@Bean
	fun freeShippingItemProcessor() : ItemProcessor<TrackedOrder, TrackedOrder> {
		return FreeShippingItemProcessor()
	}

	fun compositeItemProcessor() : ItemProcessor<Order, TrackedOrder> {
		return CompositeItemProcessorBuilder<Order, TrackedOrder>()
			.delegates(orderValidationItemProcessor(), trackedOrderItemProcessor(), freeShippingItemProcessor())
			.build()

	}

	@Bean
	fun chunkBasedStep(): Step {
		return stepBuilderFactory.get("chunkBasedStep")
			.chunk<Order,TrackedOrder>(10)
			.reader(itemReaderJdbcPagingItemReader())
			.processor(compositeItemProcessor())
			.faultTolerant()
			.skip(OrderProcessingException::class.java)
			.skipLimit(5)
			.listener(CustomSkipListener())
			.writer(jsonFileItemWriter())
			.build()
	}

	/*@Scheduled(cron = "0/15 * * * * *")*/
	fun runJob() {
		val paramBuilder = JobParametersBuilder()

		paramBuilder.addDate("runTime", Date())

		jobLauncher.run(jobTest(), paramBuilder.toJobParameters())

	}

	@Bean
	fun trigger() : Trigger {
		val scheduleBuilder = SimpleScheduleBuilder
			.simpleSchedule()
			.withIntervalInSeconds(15)
			.repeatForever()

		return TriggerBuilder.newTrigger()
			.forJob(jobDetail())
			.withSchedule(scheduleBuilder)
			.build()
	}

	@Bean
	fun jobDetail() : JobDetail {
		return JobBuilder
			.newJob(SpringBatchCourseApplication::class.java)
			.storeDurably()
			.build()
	}

	override fun executeInternal(context: JobExecutionContext) {
		val parameters = JobParametersBuilder(jobExplorer)
			.getNextJobParameters(jobTest())
			.toJobParameters()

		jobLauncher.run(jobTest(), parameters)
	}

	@Bean
	fun stepTest(): Step {
		return stepBuilderFactory.get("stepTest").tasklet { stepContribution, chunkContext ->
			println("The run time is: ${LocalDateTime.now()}")
			RepeatStatus.FINISHED
		}.build()
	}

	@Bean
	fun jobTest() : Job {
		return jobBuilderFactory.get("jobTest").incrementer(RunIdIncrementer()).start(stepTest()).build()
	}

}

fun main(args: Array<String>) {
	println("ARGUMENTS:")
	val newArgs = arrayOf("item=shoes", "run.date=" + LocalDateTime.now().toString())
	newArgs.forEach {
		println(it)
	}
	runApplication<SpringBatchCourseApplication>(*newArgs)
}

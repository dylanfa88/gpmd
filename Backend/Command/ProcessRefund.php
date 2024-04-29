<?php

namespace App\Command;

use Exception;
use DateTimeImmutable;
use App\Entity\Queue;
use App\Handler\QueueHandler;
use Gpmd\ShoplineBundle\Dto\Order\Fulfillment;
use Gpmd\ShoplineBundle\Dto\Order\LineItem;
use Gpmd\ShoplineBundle\Dto\Order\Order;
use Gpmd\ShoplineBundle\Dto\Order\PaymentDetail;
use Gpmd\ShoplineBundle\Dto\Order\Return\ReturnF;
use Gpmd\ShoplineBundle\Dto\Order\Return\ReturnLineItem;
use Gpmd\ShoplineBundle\Dto\Order\Returns;
use Gpmd\ShoplineBundle\Interfaces\Handler\OrderApiHandlerInterface;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\Persistence\ManagerRegistry;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand(
    name: 'process:refund',
    description: 'Process returned items and refund the order items'
)]
class ProcessRefund extends Command
{
    public function __construct(
        private readonly LoggerInterface $logger,
        protected readonly ManagerRegistry $doctrine,
        protected readonly EntityManagerInterface $entityManager,
        protected readonly OrderApiHandlerInterface $orderApiHandler,
        protected readonly QueueHandler $queueHandler,
    ) {
        parent::__construct();
    }

    /**
     * Execute the command logic
     *
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int
     */
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Get unprocessed items
        $queue = $this->doctrine->getRepository(Queue::class)->findUnprocessedReturns();
        if (!$queue) {
            $output->write('Nothing to process.');
            return Command::SUCCESS;
        }
        $queue = $this->mergeQueues($queue);

        // Process the queue
        /** @var Queue $item */
        foreach ($queue as $salesOrderNumber => $items) {
            try {
                // Get the order
                $order = $this->getOrderDetails($salesOrderNumber);

                // Handle the Returns
                $this->handleReturn($order, $items);

                // Handle the Refunds
                $this->handleRefund($order, $items);

                // Close the Returns
                $this->closeReturns($order);

                // Set items as processed
                $this->markItemsProcessed($items);

            } catch (Exception $e) {

                foreach ($items['queue_id'] as $queueId) {
                    $item = $this->entityManager->getReference(Queue::class, $queueId);
                    $item->setError($e->getMessage());
                }
                $this->logger->error($e->getMessage());

            } finally {

                $this->entityManager->flush();

            }
        }
        return Command::SUCCESS;
    }

    /**
     * Sub-code to handle returns
     *
     * @param Order $order
     * @param array $payload
     * @return void
     * @throws Exception
     */
    private function handleReturn(Order $order, array $payload): void
    {
        try {
            $returns = $this->orderApiHandler->getReturn($order->getId());
            $used = [];
            if ($returns) {
                /** @var Return $return */
                foreach ($returns->getReturns() as $return) {
                    /** @var ReturnLineItem $returnLineItem */
                    foreach ($return->getReturnLineItems() as $returnLineItem) {
                        $returnedQty = $returnLineItem->getQuantity();
                        $fulfillment = $returnLineItem->getFulfillmentLineItem();
                        $fulfillmentId = $returnLineItem->getFulfillmentId();
                        $sku = $fulfillment->getLineItem()->getSku();
                        $used[$fulfillmentId][$sku] = $returnedQty;
                    }
                }
            }
            $fulfillments = $this->orderApiHandler->getFulfillments($order->getId());
            $returnPayload = [
                'notify_customer' => false,
                'order_seq' => $order->getId(),
                'processed_at' => (new DateTimeImmutable())->format('Y-m-d\TH:i:sP'),
                'return_line_items' => [],
            ];
            foreach ($payload['line_items'] as $sku => $quantities) {
                foreach ($quantities as $qty) {
                    /** @var Fulfillment $fulfillment */
                    foreach ($fulfillments->getFulfillments() as $fulfillment) {
                        if ($qty == 0) {
                            break;
                        }
                        $fulfillmentId = $fulfillment->getId();
                        /** @var LineItem $lineItem */
                        foreach ($fulfillment->getLineItems() as $lineItem) {
                            $lineItemId = $lineItem->getId();
                            $lineItemFulfillmentQuantity = $lineItem->getFulfillmentQuantity();
                            if ($sku != $lineItem->getSku()) {
                                continue;
                            }
                            $usedQty = $used[$fulfillmentId][$sku] ?? 0;
                            if ($usedQty == 0 && $lineItemFulfillmentQuantity == $qty) {
                                $returnPayload['return_line_items'][] = [
                                    'fulfillment_id' => $fulfillmentId,
                                    'line_item_id' => $lineItemId,
                                    'quantity' => (int) $qty,
                                ];
                                $used[$fulfillmentId][$sku] = $qty;
                                break;
                            } elseif ($usedQty < $lineItemFulfillmentQuantity) {
                                $free = $lineItemFulfillmentQuantity - $usedQty;
                                $newQty = $qty;
                                if ($qty > $free) {
                                    $qty -= $free;
                                    $newQty = $free;
                                } else {
                                    $qty = 0;
                                }
                                $q = $returnPayload['return_line_items'][$fulfillmentId]['quantity'] ?? 0;
                                $returnPayload['return_line_items'][$fulfillmentId] = [
                                    'fulfillment_id' => $fulfillmentId,
                                    'line_item_id' => $lineItemId,
                                    'quantity' => (int) $newQty + $q,
                                ];
                                $used[$fulfillmentId][$sku] = $newQty + $usedQty;
                                if ($qty == 0) {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if (empty($returnPayload['return_line_items'])) {
                return;
            }
            $returnPayload['return_line_items'] = array_values($returnPayload['return_line_items']);
            $this->orderApiHandler->returnOrder($returnPayload);
        }
        catch (Exception $e) {
            throw new Exception($e->getMessage());
        }
    }

    /**
     * Sub-code to handle refunds
     *
     * @param Order $order
     * @param array $payload
     * @return void
     * @throws Exception
     */
    private function handleRefund(Order $order, array $payload): void
    {
        try {
            $orderId = $order->getId();
            $returns = $this->orderApiHandler->getReturn($orderId);
            $ignoreStatus = [
                'paid',
                'partially_paid',
                'partially_refunded'
            ];
            if (!in_array($order->getFinancialStatus(), $ignoreStatus)) {
                return; // nothing to refund
            }

            /** @var LineItems $lineItem */
            $refundLineItems = [];
            $amount = 0;
            foreach ($order->getLineItems() as $lineItem) {
                $itemCode = $lineItem->getSku();
                if (!isset($payload['line_items'][$itemCode])) {
                    continue;
                }
                $refundLineItems[] = [
                    'line_item_id' => $lineItem->getId(),
                    'location_id' => $lineItem->getLocationId(),
                    'quantity' => array_sum($payload['line_items'][$itemCode]),
                    'restock_type' => 'no-restock',
                    'type' => 'Returned',
                ];
                $discount = 0;
                if ($lineItem->getDiscountAllocations() !== null) {
                    foreach ($lineItem->getDiscountAllocations() as $discountAllocation) {
                        $discount += $discountAllocation->getAmount();
                    }
                }
                $amount += ($lineItem->getPrice() - $discount) * array_sum($payload['line_items'][$itemCode]);
            }

            // Calculate Amount & Shipping Refund when Shipped Qty is same as Returned Qty
            $shippingRefund = [];
            $shippingAmount = $order->getTotalShippingPriceSet()->getShopMoney()->getAmount();
            if (
                $shippingAmount > 0 &&
                $this->getShippedQty($order) === $this->getReturnedQty($returns)
            ) {
                $amount += $shippingAmount;
                $shippingRefund = [
                    'full_refund' => true,
                    'amount' => $shippingAmount
                ];
            }

            // Calculate Payment Details
            $paymentDetails = [];
            /** @var PaymentDetail $paymentDetail */
            foreach ($order->getPaymentDetails() as $paymentDetail) {
                if ($paymentDetail->getPayStatus() !== 'paid') {
                    continue;
                }
                $paymentDetails[] = [
                    'amount' => (string) $amount,
                    'currency' => '',
                    'gateway' => $paymentDetail->getPayChannel(),
                    'id' => $paymentDetail->getPaySeq(),
                    'kind' => 'refund',
                    'type' => true
                ];
            }

            // Prepare Payload
            $payload = [
                'order_id' => $orderId,
                'currency' => '',
                'note' => 'Refund',
                'notify' => true,
                'refund_line_items' => $refundLineItems,
                'transactions' => $paymentDetails,
                'shipping_refund' => $shippingRefund
            ];
            $this->orderApiHandler->refundOrder($payload);

        } catch (Exception $e) {
            throw new Exception($e->getMessage());
        }
    }

    /**
     * Close the returns
     *
     * @param Order $order
     * @return void
     * @throws Exception
     */
    private function closeReturns(Order $order): void
    {
        $returns = $this->orderApiHandler->getReturn($order->getId());
        if (isset($returns)) {
            return;
        }
        /** @var Return $item */
        foreach ($returns->getReturns() as $item) {
            if ($item->getStatus() === 'CLOSED') {
                continue;
            }
            try {
                $this->orderApiHandler->closeReturn($item->getId());
            }
            catch (Exception $e) {
                throw new Exception($e->getMessage());
            }
        }
    }

    /**
     * Close the returns
     *
     * @param array $items
     * @return void
     * @throws Exception
     */
    private function markItemsProcessed(array $items): void
    {
        foreach ($items['queue_id'] as $queueId) {
            $item = $this->entityManager->getReference(Queue::class, $queueId);
            try {
                $item->setProcessedAt(new DateTimeImmutable());
            }
            catch (Exception $e) {
                throw new Exception($e->getMessage());
            }
        }
    }

    /**
     * Get the order details
     *
     * @param int $salesOrderNumber
     * @return Order
     * @throws Exception
     */
    private function getOrderDetails(int $salesOrderNumber): Order
    {
        $orderId = $this->queueHandler->getOrderId($salesOrderNumber);
        if (!$orderId) {
            throw new Exception(sprintf('Order not found: %s', $salesOrderNumber));
        }
        return $this->queueHandler->getOrderDetails($orderId);
    }

    /**
     * Merge and return the queue array
     *
     * @param array $queue
     * @return array
     */
    private function mergeQueues(array $queue): array
    {
        $merged = [];
        foreach ($queue as $item) {
            $payload = json_decode($item->getPayload(), true);
            $items = explode(',', $payload['items']);
            $qty = explode(',', $payload['qty']);
            $salesOrderNumber = $item->getSalesOrderNumber();
            foreach ($items as $key => $sku) {
                if (!isset($merged[$salesOrderNumber]['line_items'][$sku])) {
                    $merged[$salesOrderNumber]['line_items'][$sku] = [];
                }
                $merged[$salesOrderNumber]['line_items'][$sku][] = (int) $qty[$key];
                $merged[$salesOrderNumber]['queue_id'][] = $item->getId();
            }
        }
        return $merged;
    }

    /**
     * Get shipped qty
     *
     * @param Order $order
     * @return int
     */
    private function getShippedQty(Order $order): int
    {
        $orderedQty = 0;
        /** @var Fulfillment $fulfillment */
        foreach ($order->getFulfillments() as $fulfillment) {
            foreach ($fulfillment->getLineItems() as $lineItem) {
                $orderedQty += $lineItem->getFulfillmentQuantity();
            }
        }
        return $orderedQty;
    }

    /**
     * Get returned qty
     *
     * @param ?Returns $returns
     * @return int
     */
    private function getReturnedQty(?Returns $returns): int
    {
        if (!$returns) {
            return 0;
        }
        $returnedQty = 0;
        foreach ($returns->getReturns() as $return) {
            foreach ($return->getReturnLineItems() as $returnLineItem) {
                $returnedQty += $returnLineItem->getQuantity();
            }
        }
        return $returnedQty;
    }

}
